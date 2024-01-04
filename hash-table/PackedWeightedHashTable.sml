functor PackedWeightedHashTable (structure K: KEY structure W: PACKED_WEIGHT):
  PACKED_WEIGHTED_HASH_TABLE =
struct

  structure K = K
  structure W = W

  datatype t =
    T of
      { keys: K.t array
      , emptykey: K.t
      , packedWeights: W.component array (* manually unboxed *)
      }

  exception Full
  exception DuplicateKey

  type table = t


  fun locationOfPack packedWeights i =
    ArraySlice.slice
      (packedWeights, W.NUM_COMPONENTS * i, SOME W.NUM_COMPONENTS)


  fun make {capacity, emptykey} =
    let
      val keys = SeqBasis.tabulate 5000 (0, capacity) (fn _ => emptykey)
      val packedWeights = ForkJoin.alloc (W.NUM_COMPONENTS * capacity)
      val _ = ForkJoin.parfor 1000 (0, capacity) (fn i =>
        W.unpack_into (W.z, locationOfPack packedWeights i))
    in
      T {keys = keys, emptykey = emptykey, packedWeights = packedWeights}
    end


  fun capacity (T {keys, ...}) = Array.length keys


  fun size (T {keys, emptykey, ...}) =
    SeqBasis.reduce 10000 op+ 0 (0, Array.length keys) (fn i =>
      if K.equal (Array.sub (keys, i), emptykey) then 0 else 1)


  fun unsafeViewContents (tab as T {keys, packedWeights, emptykey, ...}) =
    let
      fun makeWeight i =
        W.pack_from (locationOfPack packedWeights i)

      fun elem i =
        let val k = Array.sub (keys, i)
        in if K.equal (k, emptykey) then NONE else SOME (k, makeWeight i)
        end
    in
      DelayedSeq.tabulate elem (Array.length keys)
    end


  fun bcas (arr, i, old, new) =
    MLton.eq (old, Concurrency.casArray (arr, i) (old, new))


  fun insertCombineWeightsLimitProbes {probes = tolerance}
    (input as T {keys, packedWeights, emptykey}) (x, v) =
    let
      val n = Array.length keys

      fun claimSlotAt i = bcas (keys, i, emptykey, x)

      fun putValueAt i =
        W.unpack_atomic_combine_into (v, locationOfPack packedWeights i)

      fun loop i probes =
        if probes >= tolerance then
          raise Full
        else if i >= n then
          loop 0 probes
        else
          let
            val k = Array.sub (keys, i)
          in
            if K.equal (k, emptykey) then
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


  fun forceInsertUnique (T {keys, packedWeights, emptykey}) (x, v) =
    let
      val n = Array.length keys
      val start = (K.hash x) mod n

      fun claimSlotAt i = bcas (keys, i, emptykey, x)

      fun putValueAt i =
        W.unpack_into (v, locationOfPack packedWeights i)

      fun loop i =
        if i >= n then
          loop 0
        else
          let
            val k = Array.sub (keys, i)
          in
            if K.equal (k, emptykey) then
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


  fun lookup (T {keys, packedWeights, emptykey, ...}) x =
    let
      val n = Array.length keys
      val start = (K.hash x) mod n

      fun makeWeight i =
        W.pack_from (locationOfPack packedWeights i)

      fun loop i =
        let
          val k = Array.sub (keys, i)
        in
          if K.equal (k, emptykey) then NONE
          else if K.equal (k, x) then SOME (makeWeight i)
          else loopCheck (i + 1)
        end

      and loopCheck i =
        if i >= n then loopCheck 0 else if i = start then NONE else loop i
    in
      if n = 0 then NONE else loop start
    end

end
