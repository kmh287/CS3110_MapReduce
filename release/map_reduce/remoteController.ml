open Async.Std
open MapReduce

exception InfrastructureFailure of string
exception MapFailed             of string
exception ReduceFailed          of string

let workerList  = ref []

let init addrs =
    print_endline "start init remote controller address";
    List.iter (fun addr -> workerList := (addr::(!workerList))) addrs

module Make (Job : MapReduce.Job) = struct
    
  module WRequest = Protocol.WorkerRequest(Job)
  module WResponse = Protocol.WorkerResponse(Job)
  module C = Combiner.Make(Job)

  let connections = ref []

  let map_todo    = ref []
  let reduce_todo = ref []

  let map_results      =    Hashtbl.create 1000
  let reduce_results   =    Hashtbl.create 1000

  let connect addr = 
    Tcp.connect (Tcp.to_host_and_port (fst(addr)) (snd(addr)))

  let init_connect (socket,reader,writer) = 
      (try_with (fun () -> return (Writer.write_line writer Job.name)) )
        >>= (function
        | Core.Std.Error e -> failwith "init write fail"
        | Core.Std.Ok x -> 
            (print_endline "init write success";
            return (Some (socket, reader, writer))
            )
        )
        (* return x 
          >>= fun x ->
          print_endline "start init read";
          Reader.read_line reader  
          >>= (fun res -> 
            match res with
            | `Eof -> 
                print_endline "init connection closed";
                return None
            | `Ok(x) -> 
                print_endline "init success";
                return (Some (socket, reader, writer) )) *)
  
  let setup_connection () =
    Deferred.List.map  (!workerList)
      (fun addr ->
        (try_with (fun () -> connect addr) 
          >>= function
          | Core.Std.Error e -> 
              (print_endline "setup_connection error";
              return ())
          | Core.Std.Ok x -> 
            (print_endline "setup_connection success";
            init_connect x)
          >>= fun con ->
            (match con with
            | None -> 
              (print_endline "connection is none";
              return ();)
            | Some c -> 
              (print_endline "update connection list";
              connections := [c]@(!connections); 
              return () ) )
        ) 
      )

  let (@) xs ys = List.rev_append (List.rev xs) ys
  
  (* get a unfinished job from the todo list, and remove it from the list *)
  let get_job todo_list = 
    print_string "length of todo list is: ";
    print_int (List.length (!todo_list));
    print_newline ();
    match (!todo_list) with
    | [] -> None
    | job :: tl -> 
        begin
          todo_list := tl;
          Some job
        end

  (* insert a (id, job) into a todo list *)
  let insert_job todo_list id_job = 
    todo_list := [id_job] @ (!todo_list) 

  (* function that called in map or reduce, send request to worker and add
  the result into the result table.*)
  let compute request reader writer id phase =
    (print_endline "start compute";
    return (WRequest.send writer request)) 
    >>= (fun x ->
      print_endline "start waiting for response";
      (WResponse.receive reader) )
    >>= (fun res -> 
          print_endline "get response successfully";
          (match res with
            | `Eof -> (
              print_endline "connection closed";
              failwith "connection closed")
            | `Ok x -> 
              print_endline "connection success"; 
              return x )
        )
    >>= (fun result -> 
      match result with 
      | WResponse.MapResult(lst) ->
        begin
          print_endline "map success";
          if (not (Hashtbl.mem map_results id)) 
          then
            begin
              (match request with
              | WRequest.MapRequest(input) ->
                  return ((Hashtbl.add map_results id lst))
                  >>= (fun x -> return true)
              | _ -> 
                (print_endline 
                    "invalid request when receive map response");
                return false; )
            end
          else
            return true;
        end
      | WResponse.ReduceResult(output) ->
        begin
          print_endline "reduce success";
          if (not (Hashtbl.mem reduce_results id)) 
          then
            begin
              (match request with
              | WRequest.ReduceRequest(key, iters) ->
                  return ((Hashtbl.add reduce_results id (key, output)))
                  >>= (fun x -> return true)
              | _ -> 
                (print_endline 
                    "invalid request when receive reduce response");
                return false;)
            end
          else
            return true
        end
      | WResponse.JobFailed reason -> (
          print_endline reason;
          return false
        )
    )

  (* wrap with a map request, wait for response, and return true 
  when finishes. If the worker fails, add the job to todo list again *)
  let map reader writer id_input = 
    let (id, input) = id_input in 
    let request = WRequest.MapRequest(input) in
    print_endline "start send map request";
    (try_with 
      (fun () -> (compute request reader writer id "map")) 
      >>= (function
        | Core.Std.Error e -> 
          print_endline "map on this job fails";
          (* if this job fails, return false to tell the higher level 
          function to remove the worker and insert the job back 
          to the todo list *)
          return (insert_job map_todo id_input)
          >>= (fun x -> return false)
        | Core.Std.Ok success -> 
          if success then return true
          else raise (MapFailed "map failed")
        )
    )

  (* wrap with a reduce request, wait for response, and return () 
  when finishes. If the worker fails, add the job to todo list again *)
  let reduce reader writer id_kis =
    let (id, (key, inters)) = id_kis in 
    let request = WRequest.ReduceRequest(key, inters) in
    (try_with 
      (fun () -> (compute request reader writer id "reduce")) 
      >>= (function
        | Core.Std.Error e -> 
          print_endline "reduce on this job fails";
          (* if this job fails, return false to tell the higher level 
          function to remove the worker and insert the job back 
          to the todo list *)
          return (insert_job reduce_todo id_kis)
          >>= (fun x -> return false)
        | Core.Std.Ok success -> 
          if success then return true
          else raise (ReduceFailed "reduce failed")
        )
    )
    
  (* initialize every job with a unique id, return a (id, job) list *)
  let assign_job_id jobs = 
    let rec assign job_list id acc = match job_list with
      | [] -> acc
      | job :: tl -> assign tl (id+1) [(id, job)]@acc in
    assign jobs 0 []

  (* functions that are called in list.map, assign job to a worker,
  wait until it finish, assign another one to it until there 
  is no more jobs left *)
  let rec assign_map_job worker = 
    match (get_job map_todo) with
    | None -> 
      (print_endline "no more jobs left";
      return () )
    | Some (id, input) ->
      print_endline "get a job successfully in map phase";
      if not (Hashtbl.mem map_results id) 
      then 
        begin
          let (socket, reader, writer) = worker in
              print_endline "map on this job";
              map reader writer (id, input)
          >>= (fun success -> 
              if success then
                (print_endline "finish one job, continue working";
                assign_map_job worker
                (* >>= (fun x -> print_endline "return recursive calls";
                return ()) *) )
              else 
                (print_endline "fail on map one job, return";
                return () )
                (* return (Socket.shutdown socket `Both);
                >>= fun x -> return ()) *)
              )
        end
      else 
        assign_map_job worker

  (* same logic as assign_map_job *)
  let rec assign_reduce_job worker =
    match (get_job reduce_todo) with
    | None -> 
      (print_endline "no more jobs left";
      return () )
    | Some (id, (key, inters)) -> 
      if not (Hashtbl.mem reduce_results id) 
      then 
        begin
          let (socket, reader, writer) = worker in
            reduce reader writer (id, (key, inters))
          >>= (fun success -> 
              if success then
                (print_endline "finish one job, continue working";
                assign_reduce_job worker)
              else 
                (print_endline "fail on reduce one job, return";
                return () )
                (* return (Socket.shutdown socket `Both);
                >>= fun x -> return () ) *)
              )
        end
      else 
        assign_reduce_job worker

  let maptbl_values_to_list () = 
    List.flatten 
      (Hashtbl.fold (fun key value acc -> acc @ [value]) map_results [])

  let reducetbl_values_to_list () = 
    Hashtbl.fold (fun key value acc -> acc @ [value]) reduce_results []

  let map_reduce inputs = 
    print_endline "start map reduce";
    setup_connection () 
    >>| (fun x -> 
      print_string "connection list size:";
      print_int (List.length (!connections)); print_newline ();
      print_endline "start assign map job ids";
      assign_job_id inputs)
    >>= (fun map_job_list -> 
          print_endline "update map todo list";
          map_todo := map_job_list;
          return ();)
    >>= (fun x -> 
      print_endline "start map phase";
      Deferred.List.map ~how:`Parallel (!connections) 
        ~f:(fun worker -> assign_map_job worker)  )
    >>= (fun x -> 
        print_endline "finish map phase, start to return map values";
        (match get_job map_todo with
          | None -> (
            print_endline "start to change Hashtbl to list";
            return (maptbl_values_to_list ())
          )
          | Some _ -> 
            raise 
              (InfrastructureFailure "all worker fail during map phase")
        ))
    >>| C.combine
    >>| (fun reduce_input -> 
        print_string "length of reduce_input list is: ";
        print_int (List.length reduce_input);
        print_newline ();
        print_endline "start assign reduce job ids";
        assign_job_id reduce_input)
    >>= (fun reduce_job_list -> 
          print_endline "update map todo list";
          reduce_todo := reduce_job_list;
          return ();)
    >>= (fun x -> 
      print_endline "start reduce phase";
      Deferred.List.map ~how:`Parallel (!connections) 
        ~f:(fun worker -> assign_reduce_job worker)  )
    >>= (fun x -> 
        print_endline "finish reduce phase, start to return reduce values";
        (match get_job reduce_todo with
          | None -> return (reducetbl_values_to_list () )
          | Some _ -> 
            raise 
              (InfrastructureFailure "all worker fail during reduce phase")
        ) )
         
        

end

