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


## Results

I'm not happy with this yet. I processes 100 million rows in 16 seconds on my machine, which is a relatively beafy Apple Mac Book Pro M2.
That said, I'm a noob when it comes to writing efficient common lisp. 
The overall approach is sane I believe but I don't get down to the numbers the java implementations get.
I'm consinge quite a bit and GC time amounts to 3 seconds, allthough I tried to write low allocation code. I guess I didn't succeed :D 


  
