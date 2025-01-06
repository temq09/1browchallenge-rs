use std::{
    cmp::{max, min},
    fs::File,
    io::{self, BufReader, BufWriter, Error, Read, Write},
    sync::{mpsc, Arc, Mutex},
    thread::{self, ScopedJoinHandle},
    time::Instant,
};

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
                            data_holder.append(&raw_data.data);
                            drop(raw_data);
                        }
                        data_holder
                    }
                })
            })
            .collect::<Vec<ScopedJoinHandle<DataHolder>>>();

        let file = File::open("/home/temq/prog/workspace/tmp/1bl/1m.txt").unwrap();
        let mut reader = BufReader::new(file);
        let mut counter = 0;
        loop {
            let mut buf = vec![0; 1024 * 1000];
            let count = reader.read(buf.as_mut()).unwrap();
            if count == 0 {
                break;
            }
            let non_complete_data_index = extract_completed_data(&buf);
            let _ = reader.seek_relative((count - 1 - non_complete_data_index) as i64);

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
        println!("Result: {}", counter);
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
        hash::Hasher,
        u8,
    };

    use rustc_hash::FxHasher;

    use crate::{to_temperature, TotalReading};

    pub(crate) struct DataHolder {
        data: SplitHashMap,
        names: HashMap<u64, Vec<u8>>,
    }

    impl DataHolder {
        pub(crate) fn new() -> Self {
            DataHolder {
                data: SplitHashMap::new(),
                names: HashMap::with_capacity(100),
            }
        }

        pub(crate) fn append(&mut self, raw_data: &[u8]) {
            let mut start = 0;
            let mut middle = 0;
            for (index, element) in raw_data.iter().enumerate() {
                match element {
                    b'\n' => {
                        if index == 0 && middle == 0 {
                            println!("empty line on {}", index);
                            panic!("unexpected state");
                        }
                        let temperature = to_temperature(&raw_data[(middle + 1)..index]);
                        update_temperature(&raw_data[start..middle], temperature, self);
                        start = index + 1;
                    }
                    b';' => middle = index,
                    _ => {}
                }
            }
        }

        pub(crate) fn merge(&mut self, data: DataHolder) {
            self.data.merge(data.data);
            self.names.extend(data.names);
        }
    }

    struct SplitHashMap {
        data: HashMap<u64, TotalReading>,
    }

    impl SplitHashMap {
        fn new() -> Self {
            SplitHashMap {
                data: HashMap::new(),
            }
        }

        pub(crate) fn get_mut(&mut self, name: &u64) -> Option<&mut TotalReading> {
            self.data.get_mut(name)
        }

        pub(crate) fn get(&self, name: &u64) -> Option<&TotalReading> {
            self.data.get(name)
        }

        fn insert(&mut self, name: u64, value: TotalReading) {
            self.data.insert(name, value);
        }

        fn merge(&mut self, other: SplitHashMap) {
            for (key, value) in other.data.iter() {
                match self.data.get_mut(key) {
                    Some(reading) => reading.add(value),
                    None => {
                        self.data.insert(*key, value.clone());
                    }
                };
            }
        }
    }

    pub(crate) fn prepare_result(data: DataHolder) -> Vec<(Vec<u8>, TotalReading)> {
        let mut names: Vec<(u64, Vec<u8>)> = data.names.into_iter().collect();
        names.sort_by_key(|el| el.1.clone());
        names
            .iter()
            .flat_map(|el| {
                data.data
                    .get(&el.0)
                    .map(|reading| (el.1.clone(), reading.clone()))
            })
            .collect()
    }

    fn update_temperature(name: &[u8], value: i16, data_holder: &mut DataHolder) {
        let hash = get_hash(name);
        let table = &mut data_holder.data;
        match table.get_mut(&hash) {
            Some(raw_value) => {
                raw_value.min_temp = min(value, raw_value.min_temp);
                raw_value.max_temp = max(value, raw_value.max_temp);
                raw_value.sum_temp += value as i64;
                raw_value.temp_reading_count += 1;
            }
            None => {
                let reading = TotalReading::new(value);
                let name = name.to_vec();
                data_holder.names.insert(hash, name);
                table.insert(hash, reading);
            }
        }
    }

    fn get_hash(data: &[u8]) -> u64 {
        let mut hasher = FxHasher::default();
        hasher.write(data);
        hasher.finish()
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

fn print_result(readings: &Vec<(Vec<u8>, TotalReading)>, writer: Box<dyn Write>) {
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
