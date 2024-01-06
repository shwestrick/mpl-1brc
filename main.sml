structure CLA = CommandLineArgs

val start_time = Time.now ()

val noOutput = CLA.parseFlag "no-output"
val verbose = CLA.parseFlag "verbose"
val filename = List.hd (CLA.positional ())
               handle _ => Util.die "missing input filename"
val showParsed = CLA.parseFlag "check-show-parsed"
val noBoundsChecks = CLA.parseFlag "unsafe-no-bounds-checks"

(* capacity 20000 should be a reasonable choice according to the spec of
 * the problem, which says that there will be at most 10000 unique station
 * names
 *)
val capacity = CLA.parseInt "table-capacity" 19997

val blockSize = CLA.parseInt "block-size" 100000

(* =======================================================================
 * a few utilities
 *)

fun vprint s =
  if verbose then print s else ()

fun reportTime msg f =
  let val (result, tm) = Util.getTime f
  in vprint (msg ^ ": " ^ Time.fmt 4 tm ^ "s\n"); result
  end

fun assert b msg =
  if b then () else Util.die msg

(* ======================================================================== *)

(* By parameterizing the whole thing by {numBytes, getBytes}, we can
 * easily enable/disable bounds checking by passing in two different versions
 * of `getByte`. See bottom of file where we instantiate Main(...).
 *)
functor Main (val numBytes: int val getByte: int -> Word8.word) =
struct

  (* ======================================================================
   * Parsing.
   *)

  val semicolon_id: Word8.word = 0wx3B (* #";" *)
  val newline_id: Word8.word = 0wxA (* #"\n" *)
  val dash_id: Word8.word = 0wx2D (* #"-" *)
  val zero_id: Word8.word = 0wx30 (* #"0" *)

  type index = int
  type station_name = string
  type measurement = int

  fun findNext c i =
    if i >= numBytes then NONE
    else if getByte i = c then SOME i
    else findNext c (i + 1)

  fun parseStationName (start: index) : int * station_name =
    let
      val stop = valOf (findNext semicolon_id start)
    in
      ( stop
      , CharVector.tabulate (stop - start, fn i =>
          Char.chr (Word8.toInt (getByte (start + i))))
      )
    end

  fun parseMeasurement (start: index) : (int * measurement) =
    let
      val stop = valOf (findNext newline_id start)

      val (start, isNeg) =
        if getByte start = dash_id then (start + 1, true) else (start, false)

      val numDigits = stop - start - 1 (* exclude the dot *)
      fun getDigit i =
        let
          val c =
            if i < numDigits - 1 then getByte (start + i)
            else getByte (start + i + 1)
        in
          Word8.toInt (c - zero_id)
        end

      val x = Util.loop (0, numDigits) 0 (fn (acc, i) => 10 * acc + getDigit i)
    in
      (stop, if isNeg then ~x else x)
    end


  fun getStationName start =
    #2 (parseStationName start)

  fun getMeasurement start =
    #2 (parseMeasurement start)


  (* ==========================================================================
   * Define the hash table type. We identify entries by their starting index.
   *)

  structure Key: KEY =
  struct
    type t = index (* int *)

    val empty = ~1

    fun compare (i, j) =
      if i = j then EQUAL
      else String.compare (getStationName i, getStationName j)

    fun equal (i1, i2) =
      if i1 = i2 then
        true
      else if i1 < 0 orelse i2 < 0 then
        false
      else
        let
          fun check_from (j1, j2) =
            if getByte j1 = getByte j2 then
              if getByte j1 = semicolon_id then true
              else check_from (j1 + 1, j2 + 1)
            else
              false
        in
          check_from (i1, i2)
        end

    fun hash start =
      let
        val definitely_stop = start + 8

        fun loop acc cursor =
          let
            val x = getByte cursor
          in
            if cursor >= definitely_stop orelse x = semicolon_id then
              acc
            else
              loop (LargeWord.orb (LargeWord.<< (acc, 0w8), Word8.toLarge x))
                (cursor + 1)
          end

        val result = loop (Word8.toLarge (getByte start)) (start + 1)
      (* val result = Util.hash64_2 result *)
      in
        Word64.toIntX result
      end
  end


  structure Weight: PACKED_WEIGHT =
  struct
    val NUM_COMPONENTS = 4
    type component = int
    type pack = {min: int, max: int, tot: int, count: int}
    type t = pack


    val z: pack =
      {min = valOf Int.maxInt, max = valOf Int.minInt, tot = 0, count = 0}


    fun combine (p1: pack, p2: pack) =
      { min = Int.min (#min p1, #min p2)
      , max = Int.max (#max p1, #max p2)
      , tot = #tot p1 + #tot p2
      , count = #count p1 + #count p2
      }


    fun unpack_into ({min, max, tot, count}: pack, output) =
      ( ArraySlice.update (output, 0, min)
      ; ArraySlice.update (output, 1, max)
      ; ArraySlice.update (output, 2, tot)
      ; ArraySlice.update (output, 3, count)
      )


    fun atomic_combine_with f (arr, i) x =
      let
        fun loop current =
          let
            val desired = f (current, x)
          in
            if desired = current then
              ()
            else
              let
                val current' =
                  MLton.Parallel.arrayCompareAndSwap (arr, i) (current, desired)
              in
                if current' = current then () else loop current'
              end
          end
      in
        loop (Array.sub (arr, i))
      end


    fun unpack_atomic_combine_into ({min, max, tot, count}: pack, output) : unit =
      let
        val (arr, start, sz) = ArraySlice.base output

      (* val _ = print
        ("unpack_atomic_combine_into " ^ Int.toString start ^ " "
         ^ Int.toString sz ^ "\n") *)
      in
        (* TODO: these could be fetchAndMin/fetchAndMax if MPL exposed these
         * as primitives.
         *)
        atomic_combine_with Int.min (arr, start) min;
        atomic_combine_with Int.max (arr, start + 1) max;

        MLton.Parallel.arrayFetchAndAdd (arr, start + 2) tot;
        MLton.Parallel.arrayFetchAndAdd (arr, start + 3) count;

        ()
      end


    fun pack_from data : pack =
      { min = ArraySlice.sub (data, 0)
      , max = ArraySlice.sub (data, 1)
      , tot = ArraySlice.sub (data, 2)
      , count = ArraySlice.sub (data, 3)
      }
  end


  structure T = PackedWeightedHashTable (structure K = Key structure W = Weight)

  (* ==========================================================================
   * do the main loop. split the input into blocks and parse each block
   * independently.
   *)

  fun main () =
    let
      val table = T.make {capacity = capacity}

      fun loop cursor stop =
        if cursor >= stop then
          ()
        else
          let
            val start = cursor
            val cursor = valOf (findNext semicolon_id cursor)
            val cursor = cursor + 1 (* get past the ";" *)
            val (cursor, m) = parseMeasurement cursor
            val cursor = cursor + 1 (* get past the newline character *)

            val weight = {min = m, max = m, tot = m, count = 1}
          in
            T.insertCombineWeights table (start, weight);
            loop cursor stop
          end

      fun findLineStart i =
        case findNext newline_id i of
          SOME i => i + 1
        | NONE => numBytes

      val numBlocks = Util.ceilDiv (numBytes) blockSize

      val _ = reportTime "process entries" (fn _ =>
        ForkJoin.parfor 1 (0, numBlocks) (fn b =>
          let
            val start = findLineStart (b * blockSize)
            val stop = findLineStart ((b + 1) * blockSize)
          in
            loop start stop
          end))

      val compacted = reportTime "compact" (fn _ =>
        DelayedSeq.toArraySeq
          (DelayedSeq.mapOption (fn xx => xx) (T.unsafeViewContents table)))

      val result = reportTime "sort" (fn _ =>
        Mergesort.sort (fn (a, b) => Key.compare (#1 a, #1 b)) compacted)

      (* ==================================================================
       * print results
       *)


      val _ = vprint
        ("num unique stations: " ^ Int.toString (Seq.length result) ^ "\n")


      fun output () =
        let
          val _ = print "{"
          val _ = Util.for (0, Seq.length result) (fn i =>
            let
              val (idx, weight as {min, max, tot, count}) = Seq.nth result i
              val name = getStationName idx
              val avg = Real.fromInt tot / Real.fromInt count / 10.0
              fun fmt r =
                let val (prefix, r) = if r < 0.0 then ("-", ~r) else ("", r)
                in prefix ^ Real.fmt (StringCvt.FIX (SOME 1)) r
                end
            in
              print
                (name ^ "=" ^ fmt (Real.fromInt min / 10.0) ^ "/" ^ fmt avg
                 ^ "/" ^ fmt (Real.fromInt max / 10.0));
              if i < Seq.length result - 1 then print ", " else ()
            end)
          val _ = print "}\n"
        in
          ()
        end

      val _ = if noOutput then () else output ()

    in
      ()
    end

end


(* ======================================================================= *)

val _ = vprint ("loading " ^ filename ^ "\n")

val contents: Word8.word Seq.t = reportTime "load file" (fn _ =>
  ReadFile.contentsBinSeq filename)

val contents =
  let val (arr, i, _) = ArraySlice.base contents
  in if i = 0 then arr else Util.die ("whoops! strip away Seq failed")
  end


(* ======================================================================= *)


structure MainWithBoundsChecks =
  Main
    (val numBytes = Array.length contents
     fun getByte i = Array.sub (contents, i))
structure MainNoBoundsChecks =
  Main
    (val numBytes = Array.length contents
     fun getByte i = Unsafe.Array.sub (contents, i))


val _ =
  if noBoundsChecks then MainNoBoundsChecks.main ()
  else MainWithBoundsChecks.main ()


val stop_time = Time.now ()
val _ = vprint
  ("\ntotal time: " ^ Time.fmt 4 (Time.- (stop_time, start_time)) ^ "s\n")
