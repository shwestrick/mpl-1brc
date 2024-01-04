signature KEY =
sig
  type t
  val equal: t * t -> bool
  val hash: t -> int
end
