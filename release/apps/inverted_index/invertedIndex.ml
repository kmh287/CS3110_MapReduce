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
  type output = string list

  let name = "index.job"
  
  module WS = Set.Make(String) 

(*map takes one file name as input. The contents of the file is split 
  into different words. Each word is added to a set. At the end, the set
  is emptied into a list and each word is paired with the file name that 
  it came from *)
  let map input : (key * inter) list Deferred.t =
    let fileName = input in
    Reader.file_contents input
    >>= fun contents -> return (AppUtils.split_words contents)
    >>= fun wordList -> return (List.fold_left 
                                  (fun acc ele -> WS.add ele acc) WS.empty wordList)
    >>= fun wordSet ->  return (List.map 
                                  (fun x -> (x,fileName)) (WS.elements wordSet) )

(*Reduce takes a word and all the filenames that the word appears in. 
  All of the heavy lifting was already done by the combiner, so returning
  the list of inters is sufficient.*)
  let reduce (key, inters) : output Deferred.t =
    return (inters)  
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
      if args = [] then failwith "No file provided"   *)
      else if List.length args > 1 then failwith "Applied to too many arguments" 
      else Reader.file_lines (List.hd args)
        >>= MR.map_reduce (*call map reduce on each of the files listed in the 
                            master file*)
        >>|output  
      
  end
end

(* register the App *)
let () = MapReduce.register_app (module App)

