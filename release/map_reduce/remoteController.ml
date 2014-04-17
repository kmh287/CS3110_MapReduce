open Async.Std
open MapReduce

exception InfrastructureFailure of string
exception MapFailed             of string
exception ReduceFailed          of string

let workerList  = ref []

let init addrs =
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
            return (Some (socket, reader, writer))
        )
  
  let setup_connection () =
    Deferred.List.map  (!workerList)
      (fun addr ->
        (try_with (fun () -> connect addr) 
          >>= function
          | Core.Std.Error e -> return ()
          | Core.Std.Ok x -> init_connect x
          >>= fun con ->
            (match con with
            | None -> return ()
            | Some c -> 
              (connections := [c]@(!connections); 
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
  let compute request reader writer id phase =
    return (WRequest.send writer request)
    >>= (fun x ->
      (WResponse.receive reader) )
    >>= (fun res -> 
          (match res with
            | `Eof -> failwith "connection closed"
            | `Ok x -> return x )
        )
    >>= (fun result -> 
      match result with 
      | WResponse.MapResult(lst) ->
        begin
          if (not (Hashtbl.mem map_results id)) 
          then
            begin
              (match request with
              | WRequest.MapRequest(input) ->
                  return ((Hashtbl.add map_results id lst))
                  >>= (fun x -> return true)
              | _ -> return false; )
            end
          else
            return true;
        end
      | WResponse.ReduceResult(output) ->
        begin
          if (not (Hashtbl.mem reduce_results id)) 
          then
            begin
              (match request with
              | WRequest.ReduceRequest(key, iters) ->
                  return ((Hashtbl.add reduce_results id (key, output)))
                  >>= (fun x -> return true)
              | _ -> return false;)
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
    (try_with 
      (fun () -> (compute request reader writer id "map")) 
      >>= (function
        | Core.Std.Error e -> 
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
    | None -> return ()
    | Some (id, input) ->
      if not (Hashtbl.mem map_results id) 
      then 
        begin
          let (socket, reader, writer) = worker in
          map reader writer (id, input)
          >>= (fun success -> 
            if success then assign_map_job worker
            else return ()
          )
        end
      else 
        assign_map_job worker

  (* same logic as assign_map_job *)
  let rec assign_reduce_job worker =
    match (get_job reduce_todo) with
    | None -> return ()
    | Some (id, (key, inters)) -> 
      if not (Hashtbl.mem reduce_results id) 
      then 
        begin
          let (socket, reader, writer) = worker in
          reduce reader writer (id, (key, inters))
          >>= (fun success -> 
            if success then assign_reduce_job worker
            else return ()
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
    setup_connection () 
    >>| (fun x -> assign_job_id inputs)
    >>= (fun map_job_list -> 
          map_todo := map_job_list;
          return ();)
    >>= (fun x -> 
      Deferred.List.map ~how:`Parallel (!connections) 
        ~f:(fun worker -> assign_map_job worker)  )
    >>= (fun x -> 
          (match get_job map_todo with
            | None -> return (maptbl_values_to_list ())
            | Some _ -> 
              raise 
                (InfrastructureFailure "all worker fail during map phase")
          )
        )
    >>| C.combine
    >>| (fun reduce_input -> assign_job_id reduce_input)
    >>= (fun reduce_job_list -> 
          reduce_todo := reduce_job_list;
          return ();)
    >>= (fun x -> 
      Deferred.List.map ~how:`Parallel (!connections) 
        ~f:(fun worker -> assign_reduce_job worker)  )
    >>= (fun x -> 
        (match get_job reduce_todo with
          | None -> return (reducetbl_values_to_list () )
          | Some _ -> 
            raise 
              (InfrastructureFailure "all worker fail during reduce phase")
        ) )

end

