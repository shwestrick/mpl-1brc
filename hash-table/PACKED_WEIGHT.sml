signature PACKED_WEIGHT =
sig
  val NUM_COMPONENTS: int

  type pack
  type t = pack
  type component

  (* a "zero" element for the combining function *)
  val z: pack

  val combine: pack * pack -> pack

  val unpack_into: pack * component ArraySlice.slice -> unit
  val unpack_atomic_combine_into: pack * component ArraySlice.slice -> unit
  val pack_from: component ArraySlice.slice -> pack
end
