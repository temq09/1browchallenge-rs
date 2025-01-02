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
    let file = File::open("/home/temq/prog/workspace/tmp/1bl/1m.txt")?;
    let mut reader = BufReader::new(file);
    let mut counter = 0;

    thread::scope(|s| {
        let (tx, rx) = mpsc::channel::<RawData>();
        let receiver = Arc::new(Mutex::new(rx));

        let results = (0..7)
            .map(|thread_id| {
                s.spawn({
                    let receiver = receiver.clone();
                    move || {
                        let mut data_holder = DataHolder::new();
                        loop {
                            let guard = receiver.lock().unwrap();
                            //println!("thread {} start", thread_id);
                            let Ok(raw_data) = guard.recv() else {
                                break;
                            };
                            drop(guard);

                            //sleep(time::Duration::from_millis(1000));

                            data_holder.append(raw_data);
                            //println!("thread {} end", thread_id);
                        }
                        data_holder
                    }
                })
            })
            .collect::<Vec<ScopedJoinHandle<DataHolder>>>();

        let mut leftovers = Vec::new();
        loop {
            let mut buf = vec![0; 1024 * 1000];
            let count = reader.read(buf.as_mut_slice()).unwrap();
            if count == 0 {
                break;
            }
            let non_complete_data_index = extract_completed_data(&buf);
            let mut new_leftovers: Vec<u8> =
                Vec::with_capacity(max(0, count - non_complete_data_index));
            new_leftovers.extend(&buf[non_complete_data_index..]);

            // send buffer to the queue to parse
            buf.truncate(non_complete_data_index);
            let raw_data = RawData::new(leftovers.clone(), buf);
            leftovers.clear();
            leftovers.extend(new_leftovers);
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
    data: Vec<u8>,
}

impl RawData {
    fn new(data_prefix: Vec<u8>, data: Vec<u8>) -> Self {
        RawData { data, data_prefix }
    }
}

#[derive(Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub struct StationName(Vec<u8>);

pub(crate) mod data_structures {
    use std::{
        cmp::{max, min},
        collections::{BTreeMap, HashMap},
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

        fn playground() {}

        pub(crate) fn append(&mut self, raw_data: RawData) {
            let mut start_index = 0;
            let data = raw_data.data.as_slice();
            if !raw_data.data_prefix.is_empty() {
                let mut buffer = Vec::new();
                buffer.extend(raw_data.data_prefix.iter().cloned());
                for ele in data {
                    start_index += 1;
                    if *ele != b'\n' {
                        buffer.push(*ele);
                    } else {
                        break;
                    }
                }
                let delimeter_index = find_symbol(b';', &buffer);
                let temperature = to_temperature(&buffer[(delimeter_index + 1)..]);
                buffer.truncate(delimeter_index);
                let name = StationName(buffer);
                update_temperature(name, temperature, &mut self.data);
            }

            let mut left = 0;
            let mut delimeter_index = 0;
            for index in start_index..data.len() {
                match data[index] {
                    b';' => delimeter_index = index,
                    b'\n' => {
                        let name = StationName(data[left..delimeter_index].to_vec());
                        let temperature = to_temperature(&data[(delimeter_index + 1)..index]);
                        update_temperature(name, temperature, &mut self.data);
                        left = index + 1;
                    }
                    _ => {}
                };
            }
        }

        pub(crate) fn append_1(&mut self, raw_data: &RawData) {
            let mut buffer = Vec::from_iter(raw_data.data_prefix.iter().cloned());

            for element in raw_data.data.as_slice() {
                match element {
                    0x0A => {
                        let delimeter_index = find_symbol(b';', buffer.as_slice());
                        let (name, value) = buffer.split_at(delimeter_index);
                        let name = StationName(name.to_vec());
                        let value = to_temperature(&value[1..]);
                        update_temperature(name, value, &mut self.data);
                        buffer.clear();
                    }
                    _ => buffer.push(*element),
                };
            }
        }

        pub(crate) fn merge(&mut self, data: DataHolder) {
            self.data.merge(data.data)
        }
    }

    struct SplitHashMap {
        data: HashMap<u64, HashMap<StationName, TotalReading>>,
    }

    impl SplitHashMap {
        fn new() -> Self {
            SplitHashMap {
                data: HashMap::new(),
            }
        }

        pub(crate) fn get_mut(&mut self, name: &StationName) -> Option<&mut TotalReading> {
            self.data
                .get_mut(&get_first_symbol(name))
                .map(|table| table.get_mut(name))?
        }

        fn insert(&mut self, name: StationName, value: TotalReading) {
            let first_symbol = get_first_symbol(&name);
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

    fn get_first_symbol(name: &StationName) -> u64 {
        name.0[0] as u64
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

    fn find_symbol(symbol: u8, data: &[u8]) -> usize {
        for element in (0..data.len()).rev() {
            if data[element] == symbol {
                return element;
            }
        }
        panic!("element {} not found", symbol);
    }

    fn update_temperature(name: StationName, value: i16, table: &mut SplitHashMap) {
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
