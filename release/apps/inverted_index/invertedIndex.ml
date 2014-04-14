open Async.Std
open Async_unix

type filename = string

(******************************************************************************)
(** {2 The Inverted Index Job}                                                *)
(******************************************************************************)

module Job = struct
  type input = filename
  type key = string (*A particular word*)
  type inter = filename 
  type output = string * string list

  let name = "index.job"
  
  module WS = Set.Make(String) 

  (*Turn a string into a list of words in the string*) 
   let rec separate acc s = 
    if s = "" then acc else 
      let trimmeds = String.trim s in (*Remove leading and trailing whitespace*)
      let length = String.length trimmeds in 
      let indexOfSpace =  try String.index trimmeds ' '
                          with Not_found -> length-1 in
      let beforeSpace = if indexOfSpace = length -1 then String.sub s 0 (indexOfSpace+1)
                                                    else String.sub s 0 (indexOfSpace) in 
      let afterSpace = String.sub s (indexOfSpace+1) (length - indexOfSpace -1) in 
      separate (beforeSpace::acc) afterSpace ;;

  let map input : (key * inter) list Deferred.t =
    let fileName = input in 
    let wordList = separate [] (Reader.file_contents input) in
    let wordSet = List.fold_left (fun acc ele -> acc.add ele) WS.empty wordList in 
    return  (List.map (wordSet.elements) (fun x -> (x,fileName)) )
    

  let reduce (key, inters) : output Deferred.t =
    inters 
end

(* register the job *)
let () = MapReduce.register_job (module Job)


(******************************************************************************)
(** {2 The Inverted Index App}                                                *)
(******************************************************************************)

module App  = struct

  let name = "index"

  (** Print out all of the documents associated with each word *)
  let output results =
    let print (word, documents) =
      print_endline (word^":");
      List.iter (fun doc -> print_endline ("    "^doc)) documents
    in

    let sorted = List.sort compare results in
    List.iter print sorted


  (** for each line f in the master list, output a pair containing the filename
      f and the contents of the file named by f.  *)
  let read (master_file : filename) : (filename * string) list Deferred.t =
    Reader.file_lines master_file >>= fun filenames ->

    Deferred.List.map filenames (fun filename ->
      Reader.file_contents filename >>= fun contents ->
      return (filename, contents)
    )

  module Make (Controller : MapReduce.Controller) = struct
    module MR = Controller(Job)

    (** The input should be a single file name.  The named file should contain
        a list of files to index. *)
    let main args = 
      if args = [] then failwith "No file provided" 
      else if List.length args > 1 then failwith "Applied to too many arguments"
      else Reader.read_lines args
        >>= MR.map_reduce
        >>|output  
      
  end
end

(* register the App *)
let () = MapReduce.register_app (module App)

