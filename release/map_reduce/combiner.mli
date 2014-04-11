(** Function for grouping output from the mappers by key *)

module Make (Job : MapReduce.Job) : sig
  val combine : (Job.key * Job.inter) list -> (Job.key * Job.inter list) list
end

