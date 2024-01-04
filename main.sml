structure CLA = CommandLineArgs

fun reportTime msg f =
  let val (result, tm) = Util.getTime f
  in print (msg ^ ": " ^ Time.fmt 4 tm ^ "s\n"); result
  end

fun assert b msg =
  if b then () else Util.die msg

(* ======================================================================= *)

val filename = List.hd (CLA.positional ())
               handle _ => Util.die "missing input filename"
val _ = print ("loading " ^ filename ^ "\n")

val contents: char Seq.t = reportTime "load file" (fn _ =>
  ReadFile.contentsSeq filename)

val (numTokens, getTokenRange) = reportTime "tokenize" (fn _ =>
  Tokenize.tokenRanges (fn c => c = #"\n" orelse c = #";") contents)

val _ = assert (numTokens mod 2 = 0) "bad file? should be even number of tokens"

val numEntries = numTokens div 2
val _ = print ("number of entries: " ^ Int.toString numEntries ^ "\n")

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

    val numDigits = stop - start - 1 (* exclude the dot *)
    fun getDigit i =
      let
        val c =
          if i < numDigits - 1 then Seq.nth contents (start + i)
          else Seq.nth contents (start + i + 1)
      in
        Char.ord c - Char.ord #"0"
      end
  in
    Util.loop (0, numDigits) 0 (fn (acc, i) => 10 * acc + getDigit i)
  end

(* =========================================================================
 * spot-check: confirm parsed correctly
 *)

val _ = Util.for (0, numEntries) (fn i =>
  print (getStationName i ^ ";" ^ Int.toString (getMeasurement i) ^ "\n"))


(* ==========================================================================
 * define the hash table type
 *)

structure Key: KEY =
struct
  type t = index (* int *)

  val empty = ~1

  fun equal (i, j) =
    i = j orelse getStationName i = getStationName j

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
      val (arr, start, _) = ArraySlice.base output
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
