use std::{
    fs::File,
    io::{BufReader, Error, Read},
    sync::Mutex,
    thread::{self, ScopedJoinHandle},
};

use crate::{data_structures::DataHolder, get_indicies};

pub(crate) fn parallel_readers(file: File) -> Result<DataHolder, Error> {
    let reader = BufReader::new(file);
    let receiver = Mutex::new(reader);

    let thread_amount = std::thread::available_parallelism().unwrap().get();
    println!("Parallelism {}", thread_amount);
    thread::scope(|s| {
        let results = (0..thread_amount)
            .map(|_| {
                s.spawn(|| {
                    let mut data_holder = DataHolder::new();
                    let mut buf = vec![0; 64 * 1024];

                    loop {
                        let mut reader = receiver.lock().unwrap();

                        let count = reader.read(buf.as_mut()).unwrap();

                        if count == 0 {
                            break;
                        }
                        let buf = &buf[..count];
                        let (non_complete_data_index, seek_to) = get_indicies(buf);
                        let _ = reader.seek_relative(-seek_to);
                        drop(reader);

                        data_holder.append(&buf[..non_complete_data_index]);
                    }

                    data_holder
                })
            })
            .collect::<Vec<ScopedJoinHandle<DataHolder>>>();

        let mut output = DataHolder::new();
        for handle in results {
            let result = handle.join().unwrap();
            output.merge(result);
        }

        Ok(output)
    })
}
