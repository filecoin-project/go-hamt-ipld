# Benchparse

`benchparse` is a lightweight tool for processing the raw output of golang benchmark runs into useful csvs.

The expected workflow:
```sh
go test -bench=. -count=2 -run=^a > all-benchmark-outputs.out
benchparse all-benchmark-outputs.out out.csv
```

Three derivative values useful for analysis are generated from the benchmark raw outputs: gas cost, effective runtime and time / 1000.  Gas cost is derived from constants kept in this package reflecting current network gas pricing.  time/1000 gives the benchmark runtime of one operation since each benchmark value is taken from 1000 iterations of a benchmark.  Effective runtime is the sum of one operation's benchmark runtime and the expected runtime of the gets and puts geneated by that operation when persisting to disk.  Get and put runtime estimations are just gas cost / 10 since 10 gas correspond to 1 nanosecond.

The output csv is of the form
```
hamt-id-a
header1,header2,...
data1,data2,...

hamt-id-b
header1,header2,...
data1,data2,...

...
```