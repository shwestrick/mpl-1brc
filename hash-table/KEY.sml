signature KEY =
sig
  type t
  val empty: t
  val compare: t * t -> order
  val equal: t * t -> bool
  val hash: t -> int
end
