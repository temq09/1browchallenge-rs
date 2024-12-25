use std::{
    cmp::{max, min},
    fs::File,
    io::{self, BufReader, BufWriter, Error, Read, Write},
    thread,
    time::Instant,
};

use data_structures::DataHolder;

fn main() {
    let start = Instant::now();

    naive_implementastion().unwrap();

    println!("Execution time: {}", start.elapsed().as_millis());
}

fn naive_implementastion() -> Result<(), Error> {
    let file = File::open("/Users/artemushakov/prog/tmp/1binput/100k.txt")?;
    let mut reader = BufReader::new(file);
    let mut counter = 0;

    thread::scope(|s| {
        //for _ in 0..6 {
        //    s.spawn(|| {
        //        println!("test");
        //    });
        //}

        let mut leftovers = Vec::new();
        let mut data_holder = DataHolder::new();
        loop {
            let mut buf = [0_u8; 1024 * 1000];
            let count = reader.read(&mut buf).unwrap();
            if count == 0 {
                break;
            }
            let non_complete_data_index = extract_completed_data(&buf);
            let mut new_leftovers: Vec<u8> =
                Vec::with_capacity(max(0, count - non_complete_data_index));
            new_leftovers.extend(&buf[non_complete_data_index..]);

            // send buffer to the queue to parse
            let raw_data = RawData::new(leftovers.clone(), buf, non_complete_data_index);
            leftovers.clear();
            leftovers.extend(new_leftovers);
            data_holder.append(&raw_data);

            counter += 1;
        }
        let result = data_structures::prepare_result(data_holder);
        print_result(&result, Box::new(io::stdout()));
    });

    println!("{}", counter);
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
    pub name: StationName,
    pub min_temp: i16,
    pub max_temp: i16,
    pub sum_temp: i64,
    pub temp_reading_count: u32,
}

impl TotalReading {
    fn new(name: StationName, tmp_value: i16) -> Self {
        TotalReading {
            name,
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
    pub data_prefix: Vec<u8>,
    data: [u8; 1024000],
    end_index: usize,
}

impl RawData {
    fn new(data_prefix: Vec<u8>, data: [u8; 1024000], end_index: usize) -> Self {
        RawData {
            data,
            end_index,
            data_prefix,
        }
    }
    fn get_full_data_slice(&self) -> &[u8] {
        &self.data[..self.end_index]
    }
}

#[derive(Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub struct StationName(Vec<u8>);

pub(crate) mod data_structures {
    use std::{
        cmp::{max, min},
        collections::HashMap,
        u8,
    };

    use crate::{to_temperature, RawData, StationName, TotalReading};

    pub(crate) struct DataHolder {
        data: SplitHashMap,
    }

    impl DataHolder {
        pub(crate) fn new() -> Self {
            DataHolder {
                data: SplitHashMap::new(),
            }
        }

        pub(crate) fn append(&mut self, raw_data: &RawData) {
            let mut buffer = Vec::from_iter(raw_data.data_prefix.iter().cloned());
            let mut table = SplitHashMap::new();

            for element in raw_data.get_full_data_slice() {
                match element {
                    0x0A => {
                        let delimeter_index = buffer
                            .iter()
                            .position(|&element| element == b';')
                            .expect("delimeter not found for");

                        let (name, value) = buffer.split_at(delimeter_index);
                        let name = StationName(name.to_vec());
                        let value = to_temperature(value);
                        match table.get_mut(&name) {
                            Some(raw_value) => {
                                raw_value.min_temp = min(value, raw_value.min_temp);
                                raw_value.max_temp = max(value, raw_value.max_temp);
                                raw_value.sum_temp += value as i64;
                                raw_value.temp_reading_count += 1;
                            }
                            None => {
                                let reading = TotalReading::new(name.clone(), value);
                                table.insert(name.to_owned(), reading);
                            }
                        };
                        buffer.clear();
                    }
                    _ => buffer.push(*element),
                };
            }
            self.data.merge(table);
        }
    }

    struct SplitHashMap {
        data: HashMap<u8, HashMap<StationName, TotalReading>>,
    }

    impl SplitHashMap {
        fn new() -> Self {
            SplitHashMap {
                data: HashMap::new(),
            }
        }

        pub(crate) fn get_mut(&mut self, name: &StationName) -> Option<&mut TotalReading> {
            name.0
                .first()
                .and_then(|symbol| self.data.get_mut(&symbol))
                .map(|table| table.get_mut(name))?
        }

        fn insert(&mut self, name: StationName, value: TotalReading) {
            let first_symbol = name.0.first().expect("Name must not be empty").to_owned();
            match self.data.get_mut(&first_symbol) {
                Some(table) => {
                    let _ = table.insert(name, value);
                }
                None => {
                    let mut table = HashMap::new();
                    table.insert(name, value);
                    let _ = self.data.insert(first_symbol, table);
                }
            };
        }

        fn merge(&mut self, other: SplitHashMap) {
            for (key, value) in other.data.iter() {
                let current_readings = match self.data.get_mut(key) {
                    Some(reading) => reading,
                    None => {
                        let table = HashMap::new();
                        self.data.insert(*key, table);
                        self.data.get_mut(key).unwrap()
                    }
                };
                for reading in value.values() {
                    match current_readings.get_mut(&reading.name) {
                        Some(current_reading) => current_reading.add(reading),
                        None => {
                            let _ = current_readings.insert(reading.name.clone(), reading.clone());
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn prepare_result(data: DataHolder) -> Vec<TotalReading> {
        let mut result: Vec<TotalReading> = data
            .data
            .data
            .values()
            .flat_map(|map| map.values())
            .cloned()
            .collect();

        result.sort_by_key(|val| val.name.clone());
        result
    }
}

fn to_temperature(raw_data: &[u8]) -> i16 {
    let multiplier: i16 = if raw_data[0].is_ascii_digit() { 1 } else { -1 };
    let normalized = raw_data
        .iter()
        .cloned()
        .filter(|symbol| symbol.is_ascii_digit())
        .collect();

    String::from_utf8(normalized)
        .map(|x| x.parse::<i16>().unwrap())
        .unwrap()
        * multiplier
}

fn print_result(readings: &Vec<TotalReading>, writer: Box<dyn Write>) {
    let mut buf_writer = BufWriter::new(writer);
    for reading in readings {
        let mean = (reading.sum_temp / (reading.temp_reading_count as i64)) as f64 / 10.0;
        buf_writer.write_all(&reading.name.0).unwrap();
        buf_writer
            .write_fmt(format_args!(
                ";{};{};{}\n",
                reading.min_temp, mean, reading.max_temp
            ))
            .unwrap();
    }
}
