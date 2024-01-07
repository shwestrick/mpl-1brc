functor Buffer
  (Args:
   sig
     val numBytes: int
     val getByte: int -> Word8.word
     val getBytes: {offset: int, buffer: Word8.word array} -> int
     val arraySub: 'a array * int -> 'a
   end):
sig
  type t
  type byte = Word8.word

  val new: {capacity: int} -> t
  val fill: t -> int -> t
  val readByte: t -> int -> t * byte
  val loop:
    t
    -> {start: int, continue: byte -> bool, z: 'a, func: 'a * byte -> 'a}
    -> t * int * 'a
end =
struct

  open Args

  datatype buffer = Buffer of {buffer: Word8.word array, offset: int, size: int}
  type t = buffer
  type byte = Word8.word


  fun new {capacity} =
    Buffer {buffer = ForkJoin.alloc capacity, offset = 0, size = 0}


  fun fill (Buffer {buffer, ...}) start =
    let val size' = getBytes {offset = start, buffer = buffer}
    in Buffer {buffer = buffer, offset = start, size = size'}
    end


  fun readByte (b as Buffer {buffer, offset, size}) i =
    if i >= offset andalso i < offset + size then
      (b, arraySub (buffer, i - offset))
    else
      readByte (fill b i) i


  fun loop (Buffer {buffer, offset, size}) {start, continue, z, func} =
    let
      fun finish offset bufferSize acc i =
        ( Buffer {buffer = buffer, size = bufferSize, offset = offset}
        , offset + i
        , acc
        )

      fun loop offset bufferSize acc i =
        if i < bufferSize then
          let
            val byte = arraySub (buffer, i)
          in
            if continue byte then
              loop offset bufferSize (func (acc, byte)) (i + 1)
            else
              finish offset bufferSize acc i
          end
        else if offset + i >= numBytes then
          finish offset bufferSize acc i
        else
          let
            val offset' = offset + bufferSize
            val bufferSize' = getBytes {offset = offset', buffer = buffer}
          in
            loop offset' bufferSize' acc 0
          end
    in
      if start < offset orelse start >= offset + size then
        (* this will immediately fill the buffer *)
        loop start 0 z 0
      else
        (* can reuse some of the existing buffer *)
        loop offset size z (start - offset)
    end

end
