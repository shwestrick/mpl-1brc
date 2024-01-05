# mpl-1brc
Using
[MPL](https://github.com/MPLLang/mpl)
to solve the *1 Billion Row Challenge*
([site](https://www.morling.dev/blog/one-billion-row-challenge/),
[GitHub](https://github.com/gunnarmorling/1brc)).

To run it, you need [`smlpkg`](https://github.com/diku-dk/smlpkg) and [`mpl`](https://github.com/MPLLang/mpl) installed.

```
$ git clone https://github.com/shwestrick/mpl-1brc
$ cd mpl-1brc
$ smlpkg sync
$ make
$ ./main @mpl procs 8 -- data/1M.txt   # example data set of 1 million measurements
```

The above command uses an included `data/1M.txt` input file which has 1 million
measurements. **For the full competition, you need the 1 billion measurements
file**. See the
[1brc GitHub repo](https://github.com/gunnarmorling/1brc) for
instructions on how to generate it.


## Current results

Here are my current results on 72 cores (144 hyperthreads):
```
$ ./main @mpl procs 144 set-affinity -- /usr3/data/1brc/measurements.txt --verbose
loading /usr3/data/1brc/measurements.txt
load file: 1.4936s
process entries: 2.6253s
compact: 0.0003s
sort: 0.0022s
num unique stations: 413
{Abha=-30.8/18.0/66.8, Abidjan=-24.4/26.0/78.0, Abéché=-21.7/29.4/79.4, ... 

total time: 4.1323s
```

This result is not directly comparable with the timings reported in the
competition, because of differences in hardware. (E.g., I'm using a
much larger number of cores here.)


## Potential improvements

- [ ] Work directly on the mmap'ed file instead of loading it into an array first.
- [ ] Use block-local hash tables to avoid contention? 