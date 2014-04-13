open Async.Std

let workerList =  Hashtbl.create 8

let init addrs =
  List.iter (fun addr -> (Hashtbl.add) workerList (fst addr) (snd addr)) addrs

module Make (Job : MapReduce.Job) = struct
  let map_reduce inputs =
    failwith "Nowhere special."

end

