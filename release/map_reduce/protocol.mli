(** The messages sent between the worker and the controller. *)

open Async.Std

(******************************************************************************)
(** {2 Marshaling and unmarshaling}                                           *)
(******************************************************************************)

(** Send or receive marshaled messages through Reader or Writer *)
module type Marshalable = sig
  type t
  val receive : Reader.t -> [`Ok of t | `Eof]  Deferred.t
  val send    : Writer.t -> t -> unit
end

(** Both the {!WorkerRequest} and {!WorkerResponse} modules below include the
    {!Marshalable} interface, meaning that you can send and receive
    {!WorkerRequest.t}s and {!WorkerResponse.t}s over the network.  For
    example, to send a [WorkerRequest.t] [msg] over the channel [w], you should
    call [WorkerRequest.send w msg]. *)


(******************************************************************************)
(** {2 Mapper/Controller protocol}                                            *)
(******************************************************************************)

(** Messages from the controller to the worker *)
module WorkerRequest (Job : MapReduce.Job) : sig
  type t =
    | MapRequest of Job.input
      (** run the map phase for an input *)

    | ReduceRequest of Job.key * Job.inter list
      (** process the values associated with the given key *)

  include Marshalable with type t := t
end

(** Messages from the worker to the controller *)
module WorkerResponse (Job : MapReduce.Job) : sig
  type t =
    | JobFailed of string
      (** The application threw the given exception with stacktrace *)

    | MapResult of (Job.key * Job.inter) list
      (** successful map output *)

    | ReduceResult of Job.output
      (** successful reduce output *)

  include Marshalable with type t := t
end

