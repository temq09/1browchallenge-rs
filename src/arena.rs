use std::thread::{self};
use std::{
    cell::UnsafeCell,
    fs::File,
    hint,
    io::{BufReader, Error, Read},
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
};

use crossbeam::utils::CachePadded;

use crate::{data_structures::DataHolder, get_indicies};

pub(crate) fn read_arena(file: File, chunks_per_thread: usize) -> Result<DataHolder, Error> {
    let thread_amount = std::thread::available_parallelism().unwrap().get();
    println!("Parallelism {thread_amount}");
    let pool_size = (thread_amount - 1) * chunks_per_thread;
    let mut chunks = Vec::with_capacity(pool_size);
    for _ in 0..pool_size {
        chunks.push(Arc::new(CachePadded::new(DataChunk::new())));
    }
    thread::scope(|s| {
        let mut results = Vec::with_capacity(chunks.len());

        for subchunks in chunks.chunks_exact(chunks_per_thread) {
            let handle = s.spawn(|| process_data(subchunks));
            results.push(handle);
        }
        read_data_arena(&mut BufReader::new(file), &chunks, chunks_per_thread);

        let mut output = DataHolder::new();
        for handle in results {
            let result = handle.join().unwrap();
            output.merge(result);
        }

        debug_log("Parsing done");

        Ok(output)
    })
}

fn read_data_arena(
    reader: &mut BufReader<File>,
    arenas: &[Arc<CachePadded<DataChunk>>],
    chunks_per_thread: usize,
) {
    read_data_loop(reader, arenas, chunks_per_thread);
    debug_log("Read complete, terminate arenas");
    // Data processed, set completed flag to each state
    for arena in arenas {
        arena.accuire_write();
        arena.set_state(COMPLETED, Ordering::Relaxed);
        debug_log("Terminated");
    }
    debug_log("All terminated");
}

fn read_data_loop(
    reader: &mut BufReader<File>,
    arenas: &[Arc<CachePadded<DataChunk>>],
    chunks_per_thread: usize,
) {
    debug_log("Start read data loop");
    let mut tail = [0; 106];
    let mut tail_len = 0;
    let mut start = 0;
    let mut misses: u128 = 0;
    let mut total = 0;
    loop {
        for i in (start..arenas.len()).step_by(chunks_per_thread) {
            let arena = &arenas[i];
            total += 1;

            debug_log("Acquire arena");
            if arena.get_current_state() != AWAIT_WRITE {
                misses += 1;
                continue;
            }
            arena.accuire_write();
            debug_log("Acquired");

            let buf = unsafe { &mut *(arena.data.get()) };
            buf[..tail_len].copy_from_slice(&tail[..tail_len]);
            let bytes_read = reader
                .read(&mut buf[tail_len..(tail_len + 64 * 1024)])
                .expect("No errors expected");
            if bytes_read == 0 {
                debug_log("Read complete");
                arena.set_state(COMPLETED, Ordering::Relaxed);
                println!("Acquire misses {}, total {}", misses, total);
                return;
            }
            let total_len = tail_len + bytes_read;
            let (last_new_line_index, _) = get_indicies(&buf[..total_len]);
            arena.data_len.store(last_new_line_index, Ordering::Relaxed);

            // Minus one to exclude the newline symbol to be included
            let new_tail_len = total_len - last_new_line_index - 1;
            // Plus one so the newline symbol is excluded
            tail[..(new_tail_len)].copy_from_slice(&buf[(last_new_line_index + 1)..total_len]);
            tail_len = new_tail_len;
            arena.set_state(AWAIT_READ, Ordering::Release);
            debug_log("Released");
        }
        start = (start + 1) % chunks_per_thread;
        debug_log("read data loop wrap");
    }
}

fn process_data(chunks: &[Arc<CachePadded<DataChunk>>]) -> DataHolder {
    let mut data = DataHolder::new();
    'outer: loop {
        for chunk in chunks {
            if chunk.accuire_read() {
                process_chunk(chunk, &mut data);
            } else {
                break 'outer;
            }
        }
    }

    for chunk in chunks {
        if chunk.accuire_read() {
            process_chunk(chunk, &mut data);
        }
    }

    data
}

fn process_chunk(chunk: &DataChunk, res: &mut DataHolder) {
    debug_log("Process data");
    // data_len is not required to be atomic because accuire_read has Ordering::Acquire
    // which makes HP behavior for this read
    let data_len = chunk.data_len.load(Ordering::Relaxed);
    let buf = unsafe { &(&(*chunk.data.get()))[0..data_len] };

    res.append(buf);

    // Read has completed. Relaxed because writer is not interested in any data
    // as they will be replaced anyway. So only the fact the reader is done is enough.
    chunk.set_state(AWAIT_WRITE, Ordering::Relaxed);
}

const DEBUG_LOG_ENABLED: bool = false;
fn debug_log(message: &str) {
    if DEBUG_LOG_ENABLED {
        println!("{message}");
    }
}

unsafe impl Sync for DataChunk {}

#[derive(Debug)]
struct DataChunk {
    state: AtomicU8,
    data_len: AtomicUsize,
    data: UnsafeCell<Box<[u8]>>,
}

const AWAIT_WRITE: u8 = 0; // Writer can write, reader must wait
const BUSY_WRITE: u8 = 1; // Writer has acquired the chunk, reader must wait till READ_WAIT
const AWAIT_READ: u8 = 2; // Writer has written all data, reader can acquire the chunk
const BUSY_READ: u8 = 3; // Reader has acquired the chunk, writer must wait till 
const COMPLETED: u8 = 4; // No data will be provided anymore, reader must complete

// This is a shared memory between writer thread and reader thread.
// The coordination protocol is following:
//
// The writer can only acquire the chunk from the state AWAIT_WRITE. Once acquired it must
// set BUSY_WRITE state indicating the write is currently in progress.
// Writer must not acquire the chunk from this state.
//
// Once write is done writer must set state to AWAIT_READ.
//
// Reader can acquire the chunk from this state.
// Once reader has acquired the chunk it must set state to BUSY_READ indicating the chunk is
// now belongs to the reader. Writer must not acquire the chunk from this state.
//
// When reader has done its job it must set state to AWAIT_WRITE
impl DataChunk {
    fn new() -> Self {
        DataChunk {
            state: AtomicU8::new(AWAIT_WRITE),
            data: UnsafeCell::new(vec![0; 66 * 1024].into_boxed_slice()),
            data_len: AtomicUsize::new(0),
        }
    }

    fn accuire_read(&self) -> bool {
        loop {
            // Ordering:
            // should be Release - Acquire between writing data and reading them to have HB
            // Relaxed for fail - not interested in failure value as it will be one more spin
            match self.state.compare_exchange(
                AWAIT_READ,
                BUSY_READ,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(COMPLETED) => return false,
                _ => {}
            };
            hint::spin_loop();
        }
    }

    fn accuire_write(&self) {
        // Ordering:
        // Relaxed for success - HB is not needed, just need to get the state
        loop {
            match self.state.compare_exchange(
                AWAIT_WRITE,
                BUSY_WRITE,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(COMPLETED) => return,
                _ => {}
            };
            hint::spin_loop();
        }
    }

    fn get_current_state(&self) -> u8 {
        self.state.load(Ordering::Relaxed)
    }

    fn set_state(&self, state: u8, ordering: Ordering) {
        self.state.swap(state, ordering);
    }
}
