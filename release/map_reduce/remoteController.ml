open Async.Std
open MapReduce


module Make (Job : MapReduce.Job) = struct
    
  module WRequest = Protocol.WorkerRequest(Job)

  module WResponse = Protocol.WorkerResponse(Job)

  module Combine = Combiner.Make(Job)

  let workerList  = ref []
  let connections = ref []
  let map_todo    = ref []
  let reduce_todo = ref []

  let map_doing        =    Hashtbl.create 1000
  let map_results      =    Hashtbl.create 1000
  let reduce_doing     =    Hashtbl.create 1000
  let reduce_results   =    Hashtbl.create 1000

  let init addrs =
    List.iter (fun addr -> workerList := (addr::(!workerList))) addrs

  let connect addr = 
    Tcp.connect (Tcp.to_host_and_port (fst(addr)) (snd(addr)))

  let init (socket,reader,writer) = 
      (try_with (fun () -> return (Writer.write_line writer Job.name)) )
        >>= (function
        | Core.Std.Error e -> failwith "init write fail"
        | Core.Std.Ok x -> return x  )
         >>= fun x ->
          Reader.read_line reader 
          >>= (fun res -> 
            match res with
            | `Eof -> failwith "init connection closed"
            | `Ok(x) -> 
              print_endline "init success";
              return (socket, reader, writer) ) 
  
  let setup_connection () =
    Deferred.List.iter  (!workerList)
      (fun addr ->
        (try_with (fun () -> connect addr) >>= function
          | Core.Std.Error e -> return ()
          | Core.Std.Ok x -> 
            connections := (Some (init x))::(!connections); 
            return () )
      )

  let (@) xs ys = List.rev_append (List.rev xs) ys

  let hashtbl_values_to_list hbl = 
    Hashtbl.fold (fun key value acc -> acc @ [value]) hbl []

  let get_job job_list = 
    match (!job_list) with
    | [] -> None
    | job :: tl -> 
        begin
          job_list := tl;
          Some job
        end
  
  let compute request reader writer id =
    (try_with 
      (fun () -> return (WRequest.send writer request)) 
      >>= function
        | Core.Std.Error e -> failwith "map write fail"
        | Core.Std.Ok x -> return x  )
    >>= fun x ->
      (WResponse.receive reader)
    >>= (fun res -> 
          (match res with
            | `Eof -> failwith "map connection closed"
            | `Ok x -> return x )
        )
    >>= (fun result -> 
      match result with 
      | None -> 
        begin
          print_endline "none result";
          return (); 
        end
      | Some WResponse.MapResult(lst)  ->
        begin
          print_endline "map success";
          if (not (Hashtbl.mem map_results id)) 
          then
            begin
              Hashtbl.remove map_doing id;
              Hashtbl.add map_results id lst;  
              return ();
            end
          else
            begin
              Hashtbl.remove map_doing id;
              return ();
            end
        end
      | Some WResponse.ReduceResult(output) ->
        begin
          print_endline "reduce success";
          if (not (Hashtbl.mem reduce_results id)) 
          then
            begin
              Hashtbl.remove reduce_doing id;
              Hashtbl.add reduce_results id output;  
              return ();
            end
          else
            begin
              Hashtbl.remove reduce_doing id;
              return ();
            end
        end
      | _ -> failwith "invalid response"
    )

  (* wrap with a map request, wait for response, and 
    return () when finishes  *)
  let map reader writer id_input = 
    let (id, input) = id_input in 
    let request = WRequest.MapRequest(input) in
    compute request reader writer id 

  (* wrap with a reduce request, wait for response, and 
  return () when finishes *)
  let reduce reader writer id_kis =
    let (id, key, inters) = id_kis in 
    let request = WRequest.ReduceRequest(key, inters) in
    compute request reader writer id
    
  (* initialize every job with a unique id, return a (id, job) list *)
  let assign_job_id jobs = 
    let rec assign job_list id acc = match job_list with
      | [] -> acc
      | job :: tl -> assign tl (id+1) [(id, job)]@acc in
    (* map_todo := (assign_id_input inputs) *)
    assign jobs 0 []
  
  (* functions that are called in list.map, assign job to a worker,
  wait until it finish, assign another one to it until there 
  is no more jobs left *)
  let rec assign_map_job worker = 
    match (get_job map_todo) with
    | None -> return ();
    | Some (id, input) ->
      if not (Hashtbl.mem map_results id) 
      then 
        begin
          let (socket, reader, writer) = worker in
              map reader writer (id, input)
          >>= (fun x -> 
              assign_map_job worker)
        end
      else 
        assign_map_job worker

  let rec assign_reduce_job worker =
    match (get_job reduce_todo) with
    | None -> return ();
    | Some (id, key, inters) -> 
      if not (Hashtbl.mem reduce_results id) 
      then 
        begin
          let (socket, reader, writer) = worker in
            reduce reader writer (id, key, inters)
          >>= (fun x -> 
            assign_reduce_job worker)
        end
      else 
        assign_reduce_job worker

   (* let map_reduce inputs = 
    setup_connection () 
    >>= fun x -> initialize_input inputs 
    >>|  
  


  *)


end

