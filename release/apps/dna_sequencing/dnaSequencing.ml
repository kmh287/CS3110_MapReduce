open Async.Std
open Async_unix

(******************************************************************************)
(** input and output types                                                    *)
(******************************************************************************)

type id = int
type dna_type = Read | Ref

type sequence = {
  id   : id;
  kind : dna_type;
  data : string;
}

(** Indicates a matching subsequence of the given read and reference *)
type result = {
  length   : int;

  read     : id;
  read_off : int;

  ref      : id;
  ref_off  : int;
}

(******************************************************************************)
(** file reading and writing                                                  *)
(******************************************************************************)

(** Convert a line into a sequence *)
let read_sequence line = match Str.split (Str.regexp "@") line with
  | [id; "READ"; seq] -> {id=int_of_string id; kind=Read; data=seq}
  | [id; "REF";  seq] -> {id=int_of_string id; kind=Ref;  data=seq}
  | _ -> failwith "malformed input"

(** Read in the input data *)
let read_files filenames : sequence list Deferred.t =
  if filenames = [] then failwith "No files supplied"
  else
    Deferred.List.map filenames Reader.file_lines
      >>| List.flatten
      >>| List.map read_sequence


(** Print out a single match *)
let print_result result =
  printf "read %i [%i-%i] matches reference %i [%i-%i]\n"
         result.read result.read_off (result.read_off + result.length - 1)
         result.ref  result.ref_off  (result.ref_off  + result.length - 1)

(** Write out the output data *)
let print_results results : unit =
  List.iter print_result results

(******************************************************************************)
(** Dna sequencing jobs                                                       *)
(******************************************************************************)

module Job1 = struct
  type identity = id * int * dna_type
  type input = sequence
  type key = string
  type inter = identity  
  type output = (identity * identity) list  

  let name = "dna.job1"

(*Override @ to make it a tail recursive function*)
  let (@) a b = List.rev (List.rev_append a b)
 
  let map input : (key * inter) list Deferred.t =

    (*assembleTenMers goes through a sequence and finds every continuous 10-char
    string contained in it. The string is stored as the key, and the identifying
    information for the string is stored as the inter.*)
    let rec assembleTenMers seq offset acc =
      if (String.length seq.data - offset) < 10 then failwith "malformed DNA"
      else if (String.length seq.data - offset) = 10 then 
                                     (seq.data,(seq.id,offset,seq.kind))::acc
      else assembleTenMers (seq)
                           (offset+1)
                          (((String.sub seq.data offset 10),
                            (seq.id,offset,seq.kind))::acc) in
    return (assembleTenMers input 0 []) 
 
  (*Helper to remove the third element from a tuple*) 
  let trd (a,b,c) = c 

    (*Reduce builds a list of reads and refs from the inter list, then returns 
      the cartesian product of these sets *)
  let reduce (key, inters) : output Deferred.t =
    (*Build the Ref and Read lists, the third element of each tuple is the dna_Type*)
    let refList =  List.filter (fun ele -> trd ele  = Ref ) inters in
    let readList = List.filter (fun ele -> trd ele  = Read ) inters in 
    let cartesianProduct list1 list2 = 
      let cartHelper ele = List.fold_left (fun acc x  -> (ele,x)::acc) [] list2 in 
      List.fold_left (fun acc x -> (cartHelper x)@acc) [] list1 in 
    return ( cartesianProduct refList readList ) 

end
let () = MapReduce.register_job (module Job1)



module Job2 = struct
  type identity = id * int * dna_type 
  type input = (identity * identity) list
  type key = id * id

           (*Ref offset, Read offset*)
  type inter = int * int

            (*Ref_off, Read_off, length*)
  type output = (int * int * int) list

  let name = "dna.job2"

  let fstt (a,b,c) = a
  let sndt (a,b,c) = b
  let trdt (a,b,c) = c

  let map input : (key * inter) list Deferred.t =
   return (  List.fold_left (fun acc ele ->( (fstt(fst ele), fstt(snd ele)), 
                                           (sndt (fst ele), sndt (snd ele)) )::acc) 
                                           [] input )

  let reduce (key, inters) : output Deferred.t =
    let sortInters a b = 
      let delta1 = (abs ((fst a)-(snd a))) in
      let delta2 = (abs ((fst b)-(snd b))) in
      let temp_res = compare delta1 delta2 in
      if temp_res = 0 then (compare (fst a) (fst b) )
      else temp_res
    in
    let sortedIntersList = 
      List.sort sortInters inters in 
    let rec produceMatch inters ref_offset read_offset length acc  = 
      match inters with
      |[] -> 
        (* skip the dummy head when this is a empty list *)
        if(ref_offset = (-1) ) then acc
        else (acc @ [(ref_offset, read_offset, length)])
      |hd::tl -> 
        match hd with
        |(cur_rf,cur_re) -> 
          (* skip the dummy init head *)
          if (ref_offset = -1) 
            then produceMatch tl cur_rf cur_re 10 []
          else 
            if( (cur_rf - ref_offset) = (length - 10) ) 
              && ( (cur_re - read_offset) = (length - 10) ) 
            then produceMatch tl ref_offset read_offset (length+1) acc
            else 
              begin
                produceMatch tl cur_rf cur_re 10 
                  (acc @ [(ref_offset, read_offset, length)])
              end
    in 
    return (produceMatch sortedIntersList (-1) (-1) 0 [])


end

let () = MapReduce.register_job (module Job2)



module App  = struct

  let name = "dna"

  module Make (Controller : MapReduce.Controller) = struct
    module MR1 = Controller(Job1)
    module MR2 = Controller(Job2)

    let run (input : sequence list) : result list Deferred.t =

    (*Helpers to convert the output list into a result list*)
    let rec assembleResults id1 id2 matchList acc =
      match matchList with 
      |[] -> acc
      |(off_ref, off_read,len)::tl ->  assembleResults id1 id2 tl 
                                          ({length=len;
                                           ref = id1;
                                           read = id2;
                                           ref_off = off_ref;
                                           read_off = off_read}::acc) in
    let extractMatches (key,output) =  
          match (key, output) with
          | ((id1, id2), matchList) -> 
            return (assembleResults id1 id2 matchList []) in 



      return ( input) 
      >>= MR1.map_reduce

      >>= (fun list -> Deferred.List.map list 
        (fun (key,output) -> 
          return output)) 

      >>= MR2.map_reduce

      >>= (fun list -> Deferred.List.map list 
        extractMatches)

      >>= fun list ->  return ( List.flatten list )

    let main args =
      read_files args
        >>= run
        >>| print_results
  end
end

let () = MapReduce.register_app (module App)

