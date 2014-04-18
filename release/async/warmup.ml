
let fork d f1 f2 =
  d >>= f1;
  d >>= f2;
  ()

let deferred_map l f =
  let tempList = List.map 
    (fun x -> (f x) >>=
    (fun y -> return (y))) l 
  in
  let rec helper l acc = 
      match l with
      | [] -> (return acc)
      | hd::tl -> (hd) >>= (fun x -> helper tl (acc@[x]))
  in
  helper tempList [] 
