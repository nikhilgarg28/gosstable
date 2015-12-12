# gosstable
Sorted String Tables in Go

**BENCHMARK**

**Harware**: My Macbook Pro (3 Ghz, Intel Core I7 with 8 GB 1600 MHz DDR3 RAM and an SSD drive).

**Workload**: All results on a table of 1 million key/value pairs, with each key between 20 and 100 bytes long, and each value between 20 and 200 bytes long.

**Results**:


Writing key/value pairs to disk (including sorting) : 2.5 seconds

Loading a table's index from disk to memory : 1.3 seconds

Getting the value for a key: ~90 microseconds (basically just one disk seek)

**TODO**

1. Store compressed data to reduce file size.
2. Audit and reduce data copies.
