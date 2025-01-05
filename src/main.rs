use std::{
    cmp::{max, min},
    fs::File,
    io::{self, BufReader, BufWriter, Error, Read, Write},
    sync::{mpsc, Arc, Mutex},
    thread::{self, ScopedJoinHandle},
    time::Instant,
};

use bytes::Bytes;
use data_structures::DataHolder;

fn main() {
    let start = Instant::now();

    naive_implementastion().unwrap();

    println!("Execution time: {}", start.elapsed().as_millis());
}

fn naive_implementastion() -> Result<(), Error> {
    thread::scope(|s| {
        let (tx, rx) = mpsc::channel::<RawData>();
        let receiver = Arc::new(Mutex::new(rx));

        let results = (0..8)
            .map(|_| {
                s.spawn({
                    let receiver = receiver.clone();
                    move || {
                        let mut data_holder = DataHolder::new();
                        loop {
                            let guard = receiver.lock().unwrap();
                            let Ok(raw_data) = guard.recv() else {
                                break;
                            };
                            drop(guard);
                            let bytes = Bytes::from(raw_data.data);
                            data_holder.append(bytes);
                        }
                        data_holder
                    }
                })
            })
            .collect::<Vec<ScopedJoinHandle<DataHolder>>>();

        let file = File::open("/home/temq/prog/workspace/tmp/1bl/1b.txt").unwrap();
        let mut reder = BufReader::new(file);
        let mut counter = 0;
        loop {
            let mut buf = vec![0; 1024 * 1000];
            let count = reder.read(buf.as_mut()).unwrap();
            if count == 0 {
                break;
            }
            let non_complete_data_index = extract_completed_data(&buf);

            // send buffer to the queue to parse
            buf.truncate(non_complete_data_index);
            let raw_data = RawData::new(buf);
            tx.send(raw_data).unwrap();

            counter += 1;
        }

        drop(tx);

        println!("read complete, waiting for conumer to finish");

        let mut output = DataHolder::new();
        for handle in results {
            let result = handle.join().unwrap();
            output.merge(result);
        }

        let result = data_structures::prepare_result(output);
        print_result(&result, Box::new(io::stdout()));
        println!("{}", counter);
    });

    Ok(())
}

fn extract_completed_data(data: &[u8]) -> usize {
    let mut index = data.len() - 1;
    while index != 0 {
        if data[index] == b'\n' {
            break;
        }
        index -= 1;
    }
    index
}

#[derive(Clone)]
struct TotalReading {
    pub min_temp: i16,
    pub max_temp: i16,
    pub sum_temp: i64,
    pub temp_reading_count: u32,
}

impl TotalReading {
    fn new(tmp_value: i16) -> Self {
        TotalReading {
            min_temp: tmp_value,
            max_temp: tmp_value,
            sum_temp: tmp_value as i64,
            temp_reading_count: 1,
        }
    }

    fn add(&mut self, other: &TotalReading) {
        self.max_temp = max(self.max_temp, other.max_temp);
        self.min_temp = min(self.min_temp, other.max_temp);
        self.sum_temp += other.sum_temp;
        self.temp_reading_count += other.temp_reading_count;
    }
}

struct RawData {
    data: Vec<u8>,
}

impl RawData {
    fn new(data: Vec<u8>) -> Self {
        RawData { data }
    }
}

pub(crate) mod data_structures {
    use std::{
        cmp::{max, min},
        collections::HashMap,
    };

    use bytes::{Bytes, BytesMut};

    use crate::{to_temperature, TotalReading};

    pub(crate) struct DataHolder {
        data: SplitHashMap,
    }

    impl DataHolder {
        pub(crate) fn new() -> Self {
            DataHolder {
                data: SplitHashMap::new(),
            }
        }

        pub(crate) fn append(&mut self, mut raw_data: Bytes) {
            loop {
                if raw_data.is_empty() {
                    break;
                }
                let index_delimeter = raw_data
                    .iter()
                    .position(|el| *el == b';')
                    .expect("delimeter not found");
                let name = raw_data.split_to(index_delimeter);
                let _ = raw_data.split_to(1);

                let index_new_line = raw_data
                    .iter()
                    .position(|el| *el == b'\n')
                    .unwrap_or(raw_data.len());
                let temperature_bytes = raw_data.split_to(index_new_line);
                if !raw_data.is_empty() {
                    let _ = raw_data.split_to(1);
                }
                let temperature = to_temperature(&temperature_bytes);

                update_temperature(name, temperature, &mut self.data);
            }
        }

        pub(crate) fn merge(&mut self, data: DataHolder) {
            self.data.merge(data.data)
        }
    }

    struct SplitHashMap {
        data: HashMap<Bytes, TotalReading>,
    }

    impl SplitHashMap {
        fn new() -> Self {
            SplitHashMap {
                data: HashMap::new(),
            }
        }

        pub(crate) fn get_mut(&mut self, name: &Bytes) -> Option<&mut TotalReading> {
            self.data.get_mut(name)
        }

        fn insert(&mut self, name: Bytes, value: TotalReading) {
            self.data.insert(name, value);
        }

        fn merge(&mut self, other: SplitHashMap) {
            for (key, value) in other.data.iter() {
                match self.data.get_mut(key) {
                    Some(reading) => reading.add(value),
                    None => {
                        self.data.insert(key.clone(), value.clone());
                    }
                };
            }
        }
    }

    pub(crate) fn prepare_result(data: DataHolder) -> Vec<(Bytes, TotalReading)> {
        let mut result: Vec<(Bytes, TotalReading)> = data.data.data.into_iter().collect();
        result.sort_by_key(|val| val.0.clone());
        result
    }

    fn update_temperature(name: Bytes, value: i16, table: &mut SplitHashMap) {
        match table.get_mut(&name) {
            Some(raw_value) => {
                raw_value.min_temp = min(value, raw_value.min_temp);
                raw_value.max_temp = max(value, raw_value.max_temp);
                raw_value.sum_temp += value as i64;
                raw_value.temp_reading_count += 1;
            }
            None => {
                let mut name_buf = BytesMut::zeroed(name.len());
                name_buf.copy_from_slice(&name);
                let new_name = name_buf.freeze();
                let reading = TotalReading::new(value);
                table.insert(new_name, reading);
            }
        }
    }
}

static MULTIPLIYERS: [i16; 3] = [1, 10, 100];

fn to_temperature(raw_data: &[u8]) -> i16 {
    let mut temperature = 0;
    let mut position = 0;

    for index in (0..raw_data.len()).rev() {
        let symbol = raw_data[index];
        match symbol {
            b'0'..=b'9' => {
                temperature += (symbol - 48) as i16 * MULTIPLIYERS[position];
                position += 1;
            }
            b'-' => {
                temperature *= -1;
            }
            b'.' => continue,
            _ => panic!("Unexpected symbol: {}", symbol as char),
        }
    }

    temperature
}

fn print_result(readings: &Vec<(Bytes, TotalReading)>, writer: Box<dyn Write>) {
    let mut buf_writer = BufWriter::new(writer);
    for (name, reading) in readings {
        let mean = (reading.sum_temp / (reading.temp_reading_count as i64)) as f64 / 10.0;
        buf_writer.write_all(&name).unwrap();
        buf_writer
            .write_fmt(format_args!(
                ";{};{};{}\n",
                reading.min_temp / 10,
                mean,
                reading.max_temp / 10
            ))
            .unwrap();
    }
}
