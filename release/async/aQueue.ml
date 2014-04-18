open Async.Std

type 'a t = 'a Pipe.Reader.t * 'a Pipe.Writer.t

let create () : 'a t =
  Pipe.create()

let push (q:'a t) (x:'a) =
  ignore (Pipe.write (snd q) x)

let pop  (q:'a t) =
  Pipe.read (fst q)
  >>|(fun a -> match a with
      |`Ok(x) -> x
      |`Eof -> failwith "closed")


