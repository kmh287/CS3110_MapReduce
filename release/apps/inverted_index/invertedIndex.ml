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

  let acceptableChars = ['A';'a';'B';'b';'C';'c';'D';'d';'E';'e';'F';'f';'G';
                         'g';'H';'h';'I';'i';'J';'j';'K';'k';'L';'l';'M';'m';
                         'N';'n';'O';'o';'P';'p';'Q';'q';'R';'r';'S';'s';'T';
                         't';'U';'u';'V';'v';'W';'w';'X';'x';'Y';'y';'Z';'z';] 

  let removePunct string  =
    let removePunctHelper char = 
      if List.mem char acceptableChars then char else '`' in 
    if (String.map removePunctHelper string).[String.length string -1] = '`' then 
      String.sub string 0 (String.length string -1 ) else string  

  (*Turn a string into a list of words in the string*) 
(*
  let rec separate acc s = 
    if s = "" then acc else
      let trimmeds = String.trim s in (*Remove leading and trailing whitespace*)
      let length = String.length trimmeds in 
      let indexOfSpace =  try String.index trimmeds ' '
                          with Not_found -> length-1 in
      let beforeSpace = if indexOfSpace = (length -1) then String.sub s 0 (indexOfSpace+1)
        else String.sub s 0 (indexOfSpace) in
      let afterSpace = String.sub s (indexOfSpace+1) (length - indexOfSpace -1) in 
      separate ((removePunct beforeSpace)::acc) afterSpace 
*)

  let map input : (key * inter) list Deferred.t =
    let fileName = input in
    Reader.file_contents input
    >>= fun contents -> return (AppUtils.split_words contents)
    >>= fun wordList -> return (List.fold_left (fun acc ele -> WS.add ele acc) WS.empty wordList)
    >>= fun wordSet ->  return (List.map (fun x -> (x,fileName)) (WS.elements wordSet) )
    

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
      if args = [] 
        then failwith "No file provided" 
        else 
          if List.length args > 1 
            then failwith "Applied to too many arguments"
            else Reader.file_lines (List.hd args)
            >>= MR.map_reduce
            >>|output  
        
  end
end

(* register the App *)
let () = MapReduce.register_app (module App)

