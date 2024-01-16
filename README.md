# 1BRC(L)

One billion row challenge attempted to be solved in common lisp. 
The details for the challenge are here: https://github.com/gunnarmorling/1brc


## Why?

I was just curious how far I come with this. At this point I don't even know if I can solve it in acceptable time.
We will see.

## How to run 

set the filename in `main.lisp`

  ``` shell
$ make
$ DATA_FILE=/path/to/file ./bin/1brc 
  ```
  
You can tweak the behavior by setting the `WORKER_COUNT` and `CHUNK_SIZE` variables.

## Todo 

- [x] try it with a file generated with code from the example repository 

## Results

On my machine, this version runs in ~35 seconds.
It is a relatively beefy Apple Mac Book Pro M2.
That includes roughly 3 seconds of GC time, which I think I should be able to shave off, given that the fasted Java solution takes less that this in total. 

That said, I'm a noob when it comes to writing efficient common lisp. 
The overall approach is sane I believe but I don't get down to the numbers the java implementations get.

I wont, for now, attempt to optimize this further.
