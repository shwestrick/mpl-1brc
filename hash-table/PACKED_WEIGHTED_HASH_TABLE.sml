signature PACKED_WEIGHTED_HASH_TABLE =
sig
  structure K: KEY
  structure W: PACKED_WEIGHT

  type t
  type table = t

  exception Full
  exception DuplicateKey (* only raised by forceInsertUnique *)

  val make: {capacity: int} -> table

  val size: table -> int
  val capacity: table -> int

  val insertCombineWeightsKnownHash: table -> K.t * W.t -> {hash: int} -> unit

  (* not safe for concurrency with insertions *)
  val lookup: table -> K.t -> W.t option

  (* Unsafe because underlying array is shared. If the table is mutated,
   * then the Seq would not appear to be immutable.
   *
   * Could also imagine a function `freezeViewContents` which marks the
   * table as immutable (preventing further inserts). That would be a safer
   * version of this function.
   *)
  val unsafeViewContents: table -> (K.t * W.t) option DelayedSeq.t
end
