use std::{
    fs::File,
    io::{BufReader, Error, Read},
    thread::{self, ScopedJoinHandle},
};

use crossbeam::channel::{Receiver, Sender, unbounded};

use crate::{data_structures::DataHolder, get_indicies};

pub(crate) fn single_thread_reader(file: File) -> Result<DataHolder, Error> {
    let thread_amount = std::thread::available_parallelism().unwrap().get();
    let (tx, rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = unbounded();
    println!("Parallelism {thread_amount}");
    let read_thread_rx = rx.clone();
    thread::scope(|s| {
        let read_task = s.spawn(move || {
            let mut reader = BufReader::new(file);
            read_and_send(&mut reader, &tx);
            drop(tx);

            read_blocking(read_thread_rx)
        });

        let results = (0..(thread_amount - 1))
            .map(|_| {
                let local_rx = rx.clone();
                s.spawn(move || read_blocking(local_rx))
            })
            .collect::<Vec<ScopedJoinHandle<DataHolder>>>();

        let mut output = read_task.join().unwrap();
        for handle in results {
            let result = handle.join().unwrap();
            output.merge(result);
        }

        Ok(output)
    })
}

fn read_blocking(rx: Receiver<Vec<u8>>) -> DataHolder {
    let mut global_data_holder = DataHolder::new();

    while let Ok(data) = rx.recv() {
        global_data_holder.append(&data);
    }

    global_data_holder
}

fn read_and_send(reader: &mut BufReader<File>, tx: &Sender<Vec<u8>>) {
    let mut tail = vec![0; 0];
    loop {
        let chunk_size = 64 * 1024;
        let mut data: Vec<u8> = vec![0; chunk_size];
        let read_start = tail.len();
        data[..read_start].copy_from_slice(&tail);

        if read_start == chunk_size {
            panic!("Trying to read zero");
        }

        let bytes_read = reader.read(&mut data[read_start..]).unwrap();
        if bytes_read == 0 {
            break;
        }
        let (shrink_to, _) = get_indicies(&data);
        tail = data.split_off(shrink_to);
        tail.drain(0..1);
        tx.try_send(data).unwrap();
    }
}
