open Async.Std
open MapReduce

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
  let compute request reader writer id =
    print_endline "start compute";
    (* (try_with  *)
      ((* fun () ->  *)return (WRequest.send writer request)) 
(*       >>= function
        | Core.Std.Error e -> (
          print_endline "map write fail";
          failwith "map write fail")
        | Core.Std.Ok x -> 
          print_endline "map write success";
          return ()  ) *)
    >>= (fun x ->
      print_endline "start waiting for response";
      (WResponse.receive reader) )
    >>= (fun res -> 
          print_endline "get response successfully";
          (match res with
            | `Eof -> (
              print_endline "map connection closed";
              failwith "map connection closed")
            | `Ok x -> 
              print_endline "map connection success"; 
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
              Hashtbl.add map_results id lst;  
              return ();
            end
          else
            return ();
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
              | _ -> 
                return (print_endline 
                    "invalid request when receive reduce response") )
            end
          else
            return ()
        end
      | _ -> (
        print_endline "invalid response";
        failwith "invalid response")
    )

  (* wrap with a map request, wait for response, and return () 
  when finishes. If the worker fails, add the job to todo list again *)
  let map reader writer id_input = 
    let (id, input) = id_input in 
    let request = WRequest.MapRequest(input) in
    print_endline "start send map request";
    (try_with 
      (fun () -> return (compute request reader writer id)) 
      >>= (function
        | Core.Std.Error e -> 
          print_endline "map on this job fails";
          return (insert_job map_todo id_input)
        | Core.Std.Ok x -> 
          print_endline "map on this job success";
          x )
    )

  (* wrap with a reduce request, wait for response, and return () 
  when finishes. If the worker fails, add the job to todo list again *)
  let reduce reader writer id_kis =
    let (id, (key, inters)) = id_kis in 
    let request = WRequest.ReduceRequest(key, inters) in
    (try_with 
      (fun () -> return (compute request reader writer id)) 
      >>= (function
        | Core.Std.Error e -> return (insert_job reduce_todo id_kis)
        | Core.Std.Ok x -> x )
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
    | None -> return ();
    | Some (id, input) ->
      print_endline "get a job successfully in map phase";
      if not (Hashtbl.mem map_results id) 
      then 
        begin
          let (socket, reader, writer) = worker in
              print_endline "map on this job";
              map reader writer (id, input)
          >>= (fun x -> 
              print_endline "finish one job, continue working";
              assign_map_job worker)
        end
      else 
        assign_map_job worker

  (* same logic as assign_map_job *)
  let rec assign_reduce_job worker =
    match (get_job reduce_todo) with
    | None -> return ();
    | Some (id, (key, inters)) -> 
      if not (Hashtbl.mem reduce_results id) 
      then 
        begin
          let (socket, reader, writer) = worker in
            reduce reader writer (id, (key, inters))
          >>= (fun x -> 
            assign_reduce_job worker)
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
        return (maptbl_values_to_list ()))
    >>| C.combine
    >>| (fun reduce_input -> 
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
        return (reducetbl_values_to_list ()  ) )

end

