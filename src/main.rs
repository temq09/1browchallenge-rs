#![feature(test)]
#![feature(portable_simd)]
use core::simd;
use std::{
    cmp::{max, min},
    fs::File,
    io::{self, BufReader, BufWriter, Error, Read, Write},
    path::PathBuf,
    simd::{Select, cmp::SimdPartialEq, u8x8},
    sync::Mutex,
    thread::{self, ScopedJoinHandle},
};

use clap::{Parser, ValueEnum};
use crossbeam::channel::{Receiver, Sender, unbounded};
use data_structures::DataHolder;

#[derive(Clone, Copy, ValueEnum, Debug)]
enum Mode {
    Default,
    ReadSingle,
}

#[derive(Parser, Debug)]
struct Args {
    /// The operation mode
    #[arg(short, long)]
    mode: Option<Mode>,

    /// Path to the input file
    #[arg(short, long)]
    input: PathBuf,
}

fn main() {
    let args = Args::parse();

    let file = File::open(args.input).unwrap();
    let _ = match args.mode.unwrap_or(Mode::Default) {
        Mode::Default => naive_implementastion(file),
        Mode::ReadSingle => single_thread_reader(file),
    };
}

fn single_thread_reader(file: File) -> Result<(), Error> {
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

        let result = data_structures::prepare_result(output);
        print_result(&result, Box::new(io::stdout()));
    });

    Ok(())
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

fn get_indicies(data: &[u8]) -> (usize, i64) {
    match data.iter().rposition(|char| char == &b'\n') {
        Some(pos) => (pos, (data.len() - pos) as i64),
        None => (0, data.len() as i64),
    }
}

fn read_blocking(rx: Receiver<Vec<u8>>) -> DataHolder {
    let mut global_data_holder = DataHolder::new();

    while let Ok(data) = rx.recv() {
        global_data_holder.append(&data);
    }

    global_data_holder
}

fn naive_implementastion(file: File) -> Result<(), Error> {
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

        let _result = data_structures::prepare_result(output);
    });

    Ok(())
}

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
        self.min_temp = min(self.min_temp, other.min_temp);
        self.sum_temp += other.sum_temp;
        self.temp_reading_count += other.temp_reading_count;
    }
}

pub(crate) mod data_structures {
    use std::hash::{BuildHasher, Hasher};

    use rustc_hash::FxHashMap;

    use crate::{TotalReading, to_temperature, to_temperature_manual};

    pub(crate) struct DataHolder {
        data: FxHashMap<Vec<u8>, TotalReading>,
    }

    impl DataHolder {
        pub(crate) fn new() -> Self {
            DataHolder {
                data: FxHashMap::default(),
            }
        }

        pub(crate) fn append(&mut self, raw_data: &[u8]) {
            self.append_manual_optimization(raw_data);
            //self.append_no_optimization(raw_data);
        }

        fn append_no_optimization(&mut self, raw_data: &[u8]) {
            for line in raw_data.split(|byte| byte == &b'\n') {
                let mut iter = line.split(|char| char == &b';');
                let name = iter.next().expect("Name to be available");
                let temp = iter.next();
                update_temperature(name, to_temperature(temp.unwrap()), self);
            }
        }

        fn append_manual_optimization(&mut self, raw_data: &[u8]) {
            let mut start = 0;
            let mut middle = 0;
            let mut index = 0;
            while index < raw_data.len() {
                let element = raw_data[index];
                match element {
                    b'\n' => {
                        let temperature = to_temperature_manual(&raw_data[(middle + 1)..index]);
                        update_temperature(&raw_data[start..middle], temperature, self);
                        start = index + 1;
                        index += 2; // name takes at least one byte so jump straight to the next one
                    }
                    b';' => {
                        middle = index;
                        index += 4; // temperature takes at least 3 bytes, so just straight to 4th byte
                    }
                    _ => index += 1,
                }
            }
        }

        pub(crate) fn merge(&mut self, data: DataHolder) {
            for (key, val) in data.data {
                match self.data.get_mut(&key) {
                    Some(current) => {
                        current.add(&val);
                    }
                    None => {
                        self.data.insert(key, val);
                    }
                }
            }
        }
    }

    struct Fnv1aHash(u64);
    struct Fnv1aHashBuilder;

    impl BuildHasher for Fnv1aHashBuilder {
        type Hasher = Fnv1aHash;

        fn build_hasher(&self) -> Self::Hasher {
            Fnv1aHash(0xcbf29ce484222325)
        }
    }

    impl Hasher for Fnv1aHash {
        fn finish(&self) -> u64 {
            self.0
        }

        fn write(&mut self, bytes: &[u8]) {
            for el in bytes {
                self.0 ^= (*el) as u64;
                self.0 *= 0x00000100000001b3;
            }
        }
    }

    pub(crate) fn prepare_result(data: DataHolder) -> Vec<(Vec<u8>, TotalReading)> {
        let mut output: Vec<_> = data.data.into_iter().collect();
        output.sort_by(|l, r| l.0.cmp(&r.0));
        output
    }

    fn update_temperature(name: &[u8], value: i16, data_holder: &mut DataHolder) {
        let table = &mut data_holder.data;
        match table.get_mut(name) {
            Some(raw_value) => {
                raw_value.min_temp = raw_value.min_temp.min(value);
                raw_value.max_temp = raw_value.max_temp.max(value);
                raw_value.sum_temp += value as i64;
                raw_value.temp_reading_count += 1;
            }
            None => {
                let reading = TotalReading::new(value);
                table.insert(name.to_owned(), reading);
            }
        }
    }
}

static MULTIPLIYERS: [i16; 3] = [1, 10, 100];

fn to_temperature(raw_data: &[u8]) -> i16 {
    let mut temperature = 0;

    //let _ = raw_data[0];
    let index = raw_data.len() - 1;
    temperature += (raw_data[index] - 48) as i16 * MULTIPLIYERS[0];
    temperature += (raw_data[index - 2] - 48) as i16 * MULTIPLIYERS[1];
    (0..(index - 2)).rev().for_each(|leftover_index| {
        let symbol = raw_data[leftover_index];
        match symbol {
            b'-' => temperature *= -1,
            _ => temperature += (symbol - 48) as i16 * MULTIPLIYERS[2],
        }
    });

    temperature
}

pub fn to_temperature_manual(raw_data: &[u8]) -> i16 {
    let mut mul = 1;
    let data = if raw_data[0] == b'-' {
        mul = -1;
        &raw_data[1..]
    } else {
        raw_data
    };

    let res = if data.len() == 4 {
        (data[0] - 48) as i16 * 100 + (data[1] - 48) as i16 * 10 + (data[3] - 48) as i16
    } else {
        (data[0] - 48) as i16 * 10 + (data[2] - 48) as i16
    };
    res * mul
}

fn to_temperature_simd(raw_data: &[u8]) -> i16 {
    let sign = if raw_data[0] == b'-' { -1 } else { 1 };
    let mut data = [0; 8];
    let zeros = u8x8::splat(0);
    let start_index = 8 - raw_data.len();
    data[start_index..].copy_from_slice(raw_data);
    let mut data = simd::u8x8::from_array(data);
    // covert - to 0
    let mut mask = data.simd_ne(u8x8::splat(b'-'));
    data = mask.select(data, zeros);
    // storing mask of non zero values to select them for correction later
    mask = data.simd_ne(zeros);
    // utf8 character to integer convertion
    data -= u8x8::splat(48);

    // after the convertion only the items that were non zero should be selected
    data = mask.select(data, zeros);
    data *= simd::u8x8::from_array([0, 0, 0, 0, 100, 10, 0, 1]);
    data.as_array().iter().map(|&el| el as i16).sum::<i16>() * sign
}

fn print_result(readings: &Vec<(Vec<u8>, TotalReading)>, writer: Box<dyn Write>) {
    let mut buf_writer = BufWriter::new(writer);
    for (name, reading) in readings {
        let mean = (reading.sum_temp / (reading.temp_reading_count as i64)) as f64 / 10.0;
        buf_writer.write_all(name).unwrap();
        buf_writer
            .write_fmt(format_args!(
                ";{};{};{}\n",
                reading.min_temp as f32 / 10.0,
                mean,
                reading.max_temp as f32 / 10.0
            ))
            .unwrap();
    }
}

#[cfg(test)]
mod test {
    use std::io::{BufRead, Read};

    use crate::{get_indicies, to_temperature, to_temperature_manual, to_temperature_simd};
    extern crate test;

    #[test]
    fn test_to_temperature() {
        assert_eq!(to_temperature(b"12.0"), 120);
        assert_eq!(to_temperature(b"-12.0"), -120);
        assert_eq!(to_temperature(b"1.1"), 11);
        assert_eq!(to_temperature(b"-1.1"), -11);
    }

    #[test]
    fn test_to_temperature_1() {
        assert_eq!(to_temperature_manual(b"12.0"), 120);
        assert_eq!(to_temperature_manual(b"-12.0"), -120);
        assert_eq!(to_temperature_manual(b"1.1"), 11);
        assert_eq!(to_temperature_manual(b"-1.1"), -11);
    }

    #[test]
    fn test_read() {
        let mut input = "This is very very + long string with some text in it".as_bytes();
        let mut output: Vec<u8> = vec![0; 20];
        let first = input.read(&mut output[..10]).unwrap();
        output.truncate(first);
        let second = input.read_until(b'+', &mut output).unwrap();
        output.shrink_to(first + second);
        assert_eq!(output, "This is very very +".to_string().into_bytes());
    }

    #[test]
    fn test_to_temperature_simd() {
        assert_eq!(to_temperature_simd(b"1.1"), 11);
        assert_eq!(to_temperature_simd(b"12.0"), 120);
        assert_eq!(to_temperature_simd(b"-12.0"), -120);
        assert_eq!(to_temperature_simd(b"-1.1"), -11);
    }

    #[test]
    fn test_get_indicies() {
        assert_eq!(get_indicies("aaaaa\nbbb".as_bytes()), (5, 3));
        assert_eq!(get_indicies("aaaaaaa\nbbb".as_bytes()), (7, 3));
        assert_eq!(get_indicies("aaaaaaa\n".as_bytes()), (7, 0));
        assert_eq!(get_indicies("aaaaaaa".as_bytes()), (0, 7));
    }

    #[bench]
    fn bench_temperature_simd(b: &mut test::Bencher) {
        b.iter(|| test::black_box(to_temperature_simd(b"-12.0")));
    }

    #[bench]
    fn bench_temperature(b: &mut test::Bencher) {
        b.iter(|| test::black_box(to_temperature(b"-12.0")));
    }

    #[bench]
    fn bench_temperature_manual(b: &mut test::Bencher) {
        b.iter(|| test::black_box(to_temperature_manual(b"-12.3")));
    }
}
