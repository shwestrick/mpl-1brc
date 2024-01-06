functor PackedWeightedHashTable (structure K: KEY structure W: PACKED_WEIGHT):
  PACKED_WEIGHTED_HASH_TABLE =
struct

  structure K = K
  structure W = W

  datatype t =
    T of
      {keys: K.t array, packedWeights: W.component array (* manually unboxed *)}

  exception Full
  exception DuplicateKey

  type table = t


  val contentionFactor = CommandLineArgs.parseInt "contention-factor" 8

  fun shard () =
    MLton.Parallel.processorNumber () mod contentionFactor

  (* each pack is split into two locations, to reduce contention
   *   0 <= j < contentionFactor
   *)
  fun locationOfPack capacity packedWeights i j =
    ArraySlice.slice
      ( packedWeights
      , W.NUM_COMPONENTS * (capacity * j + i)
      , SOME W.NUM_COMPONENTS
      )


  fun make {capacity} =
    let
      val keys = SeqBasis.tabulate 5000 (0, capacity) (fn _ => K.empty)
      val packedWeights = ForkJoin.alloc
        (contentionFactor * W.NUM_COMPONENTS * capacity)
      val _ = ForkJoin.parfor 1000 (0, capacity) (fn i =>
        Util.for (0, contentionFactor) (fn j =>
          W.unpack_into (W.z, locationOfPack capacity packedWeights i j)))
    in
      T {keys = keys, packedWeights = packedWeights}
    end


  fun capacity (T {keys, ...}) = Array.length keys


  fun size (T {keys, ...}) =
    SeqBasis.reduce 10000 op+ 0 (0, Array.length keys) (fn i =>
      if K.equal (Array.sub (keys, i), K.empty) then 0 else 1)


  fun unsafeViewContents (tab as T {keys, packedWeights, ...}) =
    let
      val capacity = Array.length keys
      fun makeWeight i =
        SeqBasis.foldl W.combine W.z (0, contentionFactor) (fn j =>
          W.pack_from (locationOfPack capacity packedWeights i j))

      fun elem i =
        let val k = Array.sub (keys, i)
        in if K.equal (k, K.empty) then NONE else SOME (k, makeWeight i)
        end
    in
      DelayedSeq.tabulate elem (Array.length keys)
    end


  fun bcas (arr, i, old, new) =
    MLton.eq (old, Concurrency.casArray (arr, i) (old, new))


  fun insertCombineWeightsLimitProbes {probes = tolerance}
    (input as T {keys, packedWeights}) (x, v) =
    let
      val n = Array.length keys
      (* val _ = print
        ("insertCombineWeightsLimitProbes capacity=" ^ Int.toString n ^ "\n") *)

      val j = shard ()

      fun claimSlotAt i = bcas (keys, i, K.empty, x)

      fun putValueAt i =
        W.unpack_atomic_combine_into (v, locationOfPack n packedWeights i j)

      fun loop i probes =
        if probes >= tolerance then
          raise Full
        else if i >= n then
          loop 0 probes
        else
          let
            (* val _ = print
              ("insertCombineWeightsLimitProbes.loop " ^ Int.toString i ^ "\n") *)
            val k = Array.sub (keys, i)
          in
            if K.equal (k, K.empty) then
              if claimSlotAt i then putValueAt i else loop i probes
            else if K.equal (k, x) then
              putValueAt i
            else
              loop (i + 1) (probes + 1)
          end

      val start = (K.hash x) mod (Array.length keys)
    in
      loop start 0
    end


  fun insertCombineWeights table (x, v) =
    insertCombineWeightsLimitProbes {probes = capacity table} table (x, v)


  fun forceInsertUnique (T {keys, packedWeights}) (x, v) =
    let
      val n = Array.length keys
      val start = (K.hash x) mod n

      val j = shard ()

      fun claimSlotAt i = bcas (keys, i, K.empty, x)

      fun putValueAt i =
        W.unpack_into (v, locationOfPack n packedWeights i j)

      fun loop i =
        if i >= n then
          loop 0
        else
          let
            val k = Array.sub (keys, i)
          in
            if K.equal (k, K.empty) then
              if claimSlotAt i then putValueAt i else loop i
            else if K.equal (k, x) then
              raise DuplicateKey
            else
              loopNext (i + 1)
          end

      and loopNext i =
        if i = start then raise Full else loop i
    in
      loop start
    end


  fun lookup (T {keys, packedWeights, ...}) x =
    let
      val n = Array.length keys
      val start = (K.hash x) mod n

      fun makeWeight i =
        SeqBasis.foldl W.combine W.z (0, contentionFactor) (fn j =>
          W.pack_from (locationOfPack n packedWeights i j))

      fun loop i =
        let
          val k = Array.sub (keys, i)
        in
          if K.equal (k, K.empty) then NONE
          else if K.equal (k, x) then SOME (makeWeight i)
          else loopCheck (i + 1)
        end

      and loopCheck i =
        if i >= n then loopCheck 0 else if i = start then NONE else loop i
    in
      if n = 0 then NONE else loop start
    end

end
