use std::{
    cmp::{max, min},
    fs::File,
    io::{self, BufReader, BufWriter, Error, Read, Write},
    sync::Mutex,
    thread::{self, ScopedJoinHandle},
    time::Instant,
};

use data_structures::DataHolder;

fn main() {
    let start = Instant::now();

    naive_implementastion().unwrap();

    println!("Total time: {}", start.elapsed().as_millis());
}

fn naive_implementastion() -> Result<(), Error> {
    let file = File::open("/Users/artemushakov/prog/tmp/1binput/1b.txt").unwrap();
    let reader = BufReader::new(file);
    let receiver = Mutex::new(reader);

    let start = Instant::now();

    let thread_amount = std::thread::available_parallelism().unwrap().get();
    println!("Parallelism {}", thread_amount);
    thread::scope(|s| {
        let results = (0..thread_amount)
            .map(|_| {
                s.spawn(|| {
                    let mut data_holder = DataHolder::new();
                    let mut buf = vec![0; 1024 * 1000];

                    loop {
                        let mut reader = receiver.lock().unwrap();

                        let count = reader.read(buf.as_mut()).unwrap();

                        if count == 0 {
                            break;
                        }
                        let buf = &buf[..count];
                        let non_complete_data_index = last_index_of(buf, b'\n') + 1;
                        let offset = (count - non_complete_data_index) as i64;

                        let _ = reader.seek_relative(-offset);
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

        let execution_time = start.elapsed().as_millis();

        let result = data_structures::prepare_result(output);
        print_result(&result, Box::new(io::stdout()));

        println!("Execution time {} milliseconds", execution_time);
    });

    Ok(())
}

fn last_index_of(data: &[u8], symbol: u8) -> usize {
    let mut index = data.len() - 1;
    while index != 0 {
        if data[index] == symbol {
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

pub(crate) mod data_structures {
    use std::{
        cmp::{max, min},
        hash::Hasher,
    };

    use rustc_hash::{FxHashMap, FxHasher};

    use crate::{to_temperature, TotalReading};

    pub(crate) struct DataHolder {
        data: SplitHashMap,
        names: FxHashMap<u64, Vec<u8>>,
    }

    impl DataHolder {
        pub(crate) fn new() -> Self {
            DataHolder {
                data: SplitHashMap::new(),
                names: FxHashMap::default(),
            }
        }

        pub(crate) fn append(&mut self, raw_data: &[u8]) {
            let mut start = 0;
            let mut middle = 0;
            for (index, element) in raw_data.iter().enumerate() {
                match element {
                    b'\n' => {
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
        data: FxHashMap<u64, TotalReading>,
    }

    impl SplitHashMap {
        fn new() -> Self {
            SplitHashMap {
                data: FxHashMap::default(),
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

    let mut index = raw_data.len() - 1;
    temperature += (raw_data[index] - 48) as i16 * MULTIPLIYERS[position];
    index -= 2;
    position += 1;
    temperature += (raw_data[index] - 48) as i16 * MULTIPLIYERS[position];
    (0..index).for_each(|leftover_index| {
        let symbol = raw_data[leftover_index];
        match symbol {
            b'-' => temperature *= -1,
            _ => temperature += (symbol - 48) as i16 * MULTIPLIYERS[position],
        }
    });

    temperature
}

fn print_result(readings: &Vec<(Vec<u8>, TotalReading)>, writer: Box<dyn Write>) {
    let mut buf_writer = BufWriter::new(writer);
    for (name, reading) in readings {
        let mean = (reading.sum_temp / (reading.temp_reading_count as i64)) as f64 / 10.0;
        buf_writer.write_all(name).unwrap();
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
