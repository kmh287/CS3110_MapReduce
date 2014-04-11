(** A [Controller] implementation that farms work out over the network. *)

module Make : MapReduce.Controller

(** set up the map reduce controller to connect the the provided worker addresses *)
val init : (string * int) list -> unit

