open Async.Std

let workerList =  ref []

let init addrs =
  List.iter (fun addr -> workerList := (addr::(!workerList))) addrs

module Make (Job : MapReduce.Job) = struct
  let map_reduce inputs =
    

end

