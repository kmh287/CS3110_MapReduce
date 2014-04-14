open Async.Std

let workerList =  ref []

let init addrs =
  List.iter addrs (fun addr -> workerList := (addr::(!workerList)))

module Make (Job : MapReduce.Job) = struct
    
  module WRequest = Protocol.WorkerRequest(Job)

  module WResponse = Protocol.WorkerResponse(Job)

  module Combine = Combiner.Make(Job)

  let connect addr = 
    Tcp.connect (Tcp.to_host_and_port (fst(addr)) (snd(addr)))

  let init addr = 
    try_with (fun () -> connect addr) >>= function
      | Error e -> failwith "init connect fail"
      | Ok x -> return x  >>= (fun 
      (socket,reader,writer) -> failwith " "
        (try_with (fun () -> return (Writer.write_line writer Job.name)) 
          >>= function
          | Error e -> failwith "init write fail"
          | Ok x -> return x  )
           >>= fun x ->
            Reader.read_line reader 
            >>= (fun res -> 
              match res with
              |`Eof -> failwith "init connection closed"
              |`Ok(x) -> return x ) 
      )

  let map reader writer input = 
    let request = WRequest.MapRequest(input) in
    (try_with 
      (fun () -> return (WRequest.send writer request)) 
      >>= function
        | Error e -> failwith "map write fail"
        | Ok x -> return x  )
    >>= fun x ->
      (WResponse.receive reader)
    >>= (fun res -> 
        (match res with
          | `Eof -> failwith "map connection closed"
          | `Ok x -> 
            (match x with 
            | None -> return None
            | Some WResponse.MapResult(l) -> return (Some l)
            | _ -> failwith "invalid map response" ) )
        ) 

  let reduce reader writer key inters =
    let request = WRequest.ReduceRequest(key, inters) in
    (try_with 
      (fun () -> return (WRequest.send writer request)) 
      >>= function
        | Error e -> failwith "map write fail"
        | Ok x -> return x  )
    >>= fun x ->
      (WResponse.receive reader)
    >>= (fun res -> 
        (match res with
          | `Eof -> failwith "map connection closed"
          | `Ok x -> 
            (match x with 
            | None -> return None
            | Some WResponse.ReduceResult(output) -> return (Some output)
            | _ -> failwith "invalid map response" ) )
        ) 

  let map_reduce inputs = failwith ""
    

end

