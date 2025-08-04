Rust implementation for (one billion challenge)[https://github.com/gunnarmorling/1brc].
The main focus for the implementation is to have fast but simple and readable implementation.

## Overview
The implementation starts `n` threads(depends on the available threads in OS). 

Each thread tries to read a chunk from the same file descriptor. The thread locks on the descriptor, reads data into a buffer, adjusts the position to the start of the line, releases the descriptor and continues parsing the data. At the end data from all threads merged into a single structure.

Temperature is stored as float, so to make math operations faster temperature is parsed as int and divided by 100 during data output.

