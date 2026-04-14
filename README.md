Rust implementation for [one billion challenge](https://github.com/gunnarmorling/1brc).
The main focus for the implementation is to have fast but simple and readable implementation.

## Overview
- The implementation starts `n` threads(depends on the available threads in OS). 

- Each thread tries to read a chunk from the same file descriptor. The thread locks on the descriptor, reads data into a buffer, adjusts the position to the start of the line, releases the descriptor and continues parsing the data. At the end data from all threads merged into a single structure.

- Temperature is stored as float, so to make math operations faster temperature is parsed as int and divided by 100 during data output.

## How to run

1. Build a release binary

```shell
cargo build --release
```

2. Prepare the test data following these [steps](https://github.com/gunnarmorling/1brc?tab=readme-ov-file#running-the-challenge)

3. When the input data ready run the binary

```shell
time ./target/release/onebillionrow-rs --input=/path/to/measurements.txt
```

### Different modes for data reading

- `read-single` creates a single thread to read data, and n-1 threads to process them
- `default` creates n threads, each thread acquires a shared BufReader, reads data and releases the reader
- `arena` creates a single thread to read data and n-1 threads to process the data; 
each processor thread has a pre-allocated buffer that the reader thread uses to fill the data to reduce allocation

Note: N is amount of cores available on the CPU

## Results

Dataset: 1 billion rows
The input file is not mapped to the memory, so everytime it's read from disk

### Baseline for comparison
For the baseline a java implementation from the original repo was used
```shell
./calculate_average_gonix.sh  0.00s user 0.01s system 0% cpu 1.941 total
```

### Macbook Pro
Config:
- Macbook Pro MAX M1 64GB 2021
- OS: Tahoe 26.4.1
- Cores amount: 10

Disk read speed
```shell
time dd if=1b.txt of=/dev/null bs=64k
210500+1 records in
210500+1 records out
13795335048 bytes transferred in 0.891794 secs (15469194733 bytes/sec)
dd if=1b.txt of=/dev/null bs=64k  0.05s user 0.85s system 99% cpu 0.899 total
```

Results(used `append_manual_optimization` function):
- default: `25.35s user 3.54s system 753% cpu 3.836 total`
- read-single: `27.88s user 1.73s system 959% cpu 3.087 total`
- arena: `33.84s user 1.29s system 977% cpu 3.594 total`
