signature KEY =
sig
  type t
  val empty: t
  val equal: t * t -> bool
  val hash: t -> int
end
