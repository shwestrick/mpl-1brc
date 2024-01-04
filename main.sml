structure CLA = CommandLineArgs

val verbose = CLA.parseFlag "verbose"
fun vprint s =
  if verbose then print s else ()

fun reportTime msg f =
  let val (result, tm) = Util.getTime f
  in vprint (msg ^ ": " ^ Time.fmt 4 tm ^ "s\n"); result
  end

fun assert b msg =
  if b then () else Util.die msg

(* ======================================================================= *)

val filename = List.hd (CLA.positional ())
               handle _ => Util.die "missing input filename"
val _ = vprint ("loading " ^ filename ^ "\n")

val contents: char Seq.t = reportTime "load file" (fn _ =>
  ReadFile.contentsSeq filename)

val (numTokens, getTokenRange) = reportTime "tokenize" (fn _ =>
  Tokenize.tokenRanges (fn c => c = #"\n" orelse c = #";") contents)

val _ = assert (numTokens mod 2 = 0) "bad file? should be even number of tokens"

val numEntries = numTokens div 2
val _ = vprint ("number of entries: " ^ Int.toString numEntries ^ "\n")

(* ======================================================================
 * Identify each entry by its line number
 *)

type index = int
type station_name = string
type measurement = int

fun getStationName (i: index) : station_name =
  let val (start, stop) = getTokenRange (2 * i)
  in CharVector.tabulate (stop - start, fn i => Seq.nth contents (start + i))
  end

fun getMeasurement (i: index) : measurement =
  let
    val (start, stop) = getTokenRange (2 * i + 1)

    val (start, isNeg) =
      if Seq.nth contents start = #"-" then (start + 1, true)
      else (start, false)

    val numDigits = stop - start - 1 (* exclude the dot *)
    fun getDigit i =
      let
        val c =
          if i < numDigits - 1 then Seq.nth contents (start + i)
          else Seq.nth contents (start + i + 1)
      in
        Char.ord c - Char.ord #"0"
      end

    val x = Util.loop (0, numDigits) 0 (fn (acc, i) => 10 * acc + getDigit i)
  in
    if isNeg then ~x else x
  end

(* =========================================================================
 * spot-check: confirm parsed correctly
 *)

val showParsed = CLA.parseFlag "check-show-parsed"

val _ =
  if not showParsed then
    ()
  else
    Util.for (0, numEntries) (fn i =>
      vprint (getStationName i ^ ";" ^ Int.toString (getMeasurement i) ^ "\n"))


(* ==========================================================================
 * define the hash table type
 *)

structure Key: KEY =
struct
  type t = index (* int *)

  val empty = ~1

  fun compare (i, j) =
    if i = j then EQUAL else String.compare (getStationName i, getStationName j)

  fun equal (i, j) =
    i = j
    orelse (i >= 0 andalso j >= 0 andalso getStationName i = getStationName j)

  fun hashStr str =
    let
      (* just cap at 32 for long strings *)
      val n = Int.min (32, String.size str)
      fun c i =
        Word64.fromInt (Char.ord (String.sub (str, i)))
      fun loop h i =
        if i >= n then h else loop (Word64.+ (Word64.* (h, 0w31), c i)) (i + 1)

      val result = loop 0w7 0
    in
      Util.hash64_2 result
    end

  fun hash i =
    Word64.toIntX (hashStr (getStationName i))
end


structure Weight: PACKED_WEIGHT =
struct
  val NUM_COMPONENTS = 4
  type component = int
  type pack = {min: int, max: int, tot: int, count: int}
  type t = pack


  val z: pack =
    {min = valOf Int.maxInt, max = valOf Int.minInt, tot = 0, count = 0}


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
 * do the main loop
 *)

(* capacity 20000 should be a reasonable choice according to the spec of
 * the problem, which says that there will be at most 10000 unique station
 * names
 *)
val capacity = CLA.parseInt "table-capacity" 20000

val table = T.make {capacity = capacity}

val _ = reportTime "process entries" (fn _ =>
  ForkJoin.parfor 100 (0, numEntries) (fn i =>
    let
      val m = getMeasurement i
      val weight = {min = m, max = m, tot = m, count = 1}
    in
      T.insertCombineWeights table (i, weight)
    end))

val compacted = reportTime "compact" (fn _ =>
  DelayedSeq.toArraySeq
    (DelayedSeq.mapOption (fn xx => xx) (T.unsafeViewContents table)))

val result = reportTime "sort" (fn _ =>
  Mergesort.sort (fn (a, b) => Key.compare (#1 a, #1 b)) compacted)

(* =========================================================================
 * print results
 *)


val _ = vprint
  ("num unique stations: " ^ Int.toString (Seq.length result) ^ "\n")

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
      (name ^ "=" ^ fmt (Real.fromInt min / 10.0) ^ "/" ^ fmt avg ^ "/"
       ^ fmt (Real.fromInt max / 10.0));
    if i < Seq.length result - 1 then print ", " else ()
  end)
val _ = print "}\n"
