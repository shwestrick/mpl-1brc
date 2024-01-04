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
