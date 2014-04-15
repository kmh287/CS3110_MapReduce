open Async.Std

module Make (Job : MapReduce.Job) = struct
  
  module WRequest = Protocol.WorkerRequest(Job)
  module WResponse = Protocol.WorkerResponse(Job)

  (* see .mli *)
  let run r w =  
    let rec continue_run r w =
      WRequest.receive r >>= function
          | `Eof    -> return ()
          | `Ok request -> 
            (match request with
            | WRequest.MapRequest(input) -> (Job.map input)
              >>= (fun result -> 
                let response = WResponse.MapResult(result) in
                (try_with 
                  (fun () -> return (WResponse.send w response)) 
                >>= function
                  | Core.Std.Error e -> (
                    print_endline "map response fail";
                    failwith "map response fail")
                  | Core.Std.Ok x -> return x )
              )
              >>= ( fun x ->
                continue_run r w
              )
            | WRequest.ReduceRequest(key, inters) -> (Job.reduce (key, inters) )
              >>= (fun result -> 
                let response = WResponse.ReduceResult(result) in
                (try_with 
                  (fun () -> return (WResponse.send w response)) 
                >>= function
                  | Core.Std.Error e -> (
                    print_endline "map response fail";
                    failwith "map response fail")
                  | Core.Std.Ok x -> return x )
              )
              >>= ( fun x ->
                continue_run r w
              )
          )
  in continue_run r w

end

(* see .mli *)
let init port =
  Tcp.Server.create
    ~on_handler_error:`Raise
    (Tcp.on_port port)
    (fun _ r w ->
      Reader.read_line r >>= function
        | `Eof    -> return ()
        | `Ok job -> match MapReduce.get_job job with
          | None -> return ()
          | Some j ->
            let module Job = (val j) in
            let module Worker = Make(Job) in
            Worker.run r w
    )
    >>= fun _ ->
  print_endline "server started";
  print_endline "worker started.";
  print_endline "registered jobs:";
  List.iter print_endline (MapReduce.list_jobs ());
  never ()


