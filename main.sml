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

val numLines = numTokens div 2
val _ = print ("number of entries: " ^ Int.toString numLines ^ "\n")
