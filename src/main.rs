use std::{
    fs::File,
    io::{BufReader, Error, Read},
    thread,
    time::Instant,
};

fn main() {
    let start = Instant::now();

    naive_implementastion().unwrap();

    println!("Execution time: {}", start.elapsed().as_millis());
}

fn naive_implementastion() -> Result<(), Error> {
    let file = File::open("/Users/artemushakov/prog/tmp/1billion/1brc/measurements.txt")?;
    let mut reader = BufReader::new(file);
    let mut counter = 0;

    thread::scope(|s| {
        for _ in 0..6 {
            s.spawn(|| {
                println!("test");
            });
        }

        let mut buf = [0_u8; 1024 * 100];
        loop {
            let count = reader.read(&mut buf).unwrap();
            if count == 0 {
                break;
            }
            counter += 1;
        }
    });

    println!("{}", counter);
    Ok(())
}

fn extract_completed_data(data: &[u8]) {}

struct StationReading {
    pub name: String,
    pub temperature: i16,
}

struct TotalReading {
    pub name: String,
    pub min_temp: i16,
    pub max_temp: i16,
    pub mean_temp: i16,
}
