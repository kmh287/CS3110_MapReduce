open Async.Std

module Make (Job : MapReduce.Job) = struct
  
  module WRequest = Protocol.WorkerRequest(Job)
  module WResponse = Protocol.WorkerResponse(Job)

  (* see .mli *)
  let run r w =  
    let rec continue_run r w =
      print_endline "worker start wait for request";
      WRequest.receive r >>= function
          | `Eof    -> return ()
          | `Ok request -> 
            print_endline "worker received a request";
            (match request with
            | WRequest.MapRequest(input) -> 
              (print_endline "worker received a map request";
              Job.map input)
              >>= (fun result -> 
                print_endline "worker caculate map successfully";
                let response = WResponse.MapResult(result) in
                print_endline "start to send map response back";
                (try_with 
                  (fun () -> return (WResponse.send w response)) 
                >>= function
                  | Core.Std.Error e -> (
                    print_endline "map response fail";
                    failwith "map response fail")
                  | Core.Std.Ok x -> (
                    print_endline "map response success";
                    return x ) )
              )
              >>= ( fun x -> continue_run r w)
            | WRequest.ReduceRequest(key, inters) -> 
              (print_endline "worker received a reduce request";
              Job.reduce (key, inters))
              >>= (fun result -> 
                print_endline "worker caculate reduce successfully";
                let response = WResponse.ReduceResult(result) in
                print_endline "start to send map response back";
                (try_with 
                  (fun () -> return (WResponse.send w response)) 
                >>= function
                  | Core.Std.Error e -> (
                    print_endline "map response fail";
                    failwith "map response fail")
                  | Core.Std.Ok x -> (
                    print_endline "reduce response success";
                    return x ) )
              )
              >>= ( fun x -> 
                print_endline "continue run";
                continue_run r w )
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


