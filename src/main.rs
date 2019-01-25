extern crate chrono;
extern crate crossbeam_channel;
extern crate fern;
extern crate itertools;
#[macro_use] extern crate log;
extern crate serde_json;

use itertools::Itertools;
use serde_json::{Map, Value};

use std::fs;
use std::io::{BufRead, BufReader};
// use std::mem::size_of;
use std::thread;
use std::time::Instant;

extern crate ds;
use ds::{Data, conf};

fn perform(filename: &str) {
    info!("started `{}`", filename);

    let file = fs::File::open(filename).unwrap();

    let mut instant = Instant::now();

    let mut data = Data::new();

    let (tx_ch, rx_ch): (
        crossbeam_channel::Sender<Vec<String>>,
        crossbeam_channel::Receiver<Vec<String>>
    ) = crossbeam_channel::bounded(conf::vals.threads * 4);
    let (tx_res, rx_res) = crossbeam_channel::unbounded();

    for _ in 0..conf::vals.threads {
        let rx_ch = rx_ch.clone();
        let tx_res = tx_res.clone();
        thread::spawn(move || {
            let mut data = Data::new();
            for chunk in rx_ch {
                for line in chunk {
                    let v: Map<String, Value> = serde_json::from_str(&line).unwrap();
                    for (key, value) in v {
                        match value {
                            Value::Number(n) => {
                                if n.is_i64() {
                                    data.add_i(key, n.as_i64().unwrap());
                                } else if n.is_f64() {
                                    data.add_f(key, n.as_f64().unwrap());
                                }
                            },
                            Value::String(s) => {
                                data.add_s(key, s);
                            },
                            _ => (),
                        }
                    }
                }
            }
            tx_res.send(data).unwrap();
            drop(tx_res);
        });
    }

    drop(rx_ch);
    drop(tx_res);

    for chunk_iter in &BufReader::new(file).lines().chunks(1000) {
        let chunk: Vec<String> = chunk_iter.map(|c| c.unwrap()).collect();
        tx_ch.send(chunk).unwrap();
    }
    drop(tx_ch);

    for part in rx_res {
        data.append(part);
    }

    data.preaggregate();

    debug!("json `{:?}`", instant.elapsed());
    instant = Instant::now();

    data.sort();

    debug!("sort `{:?}`", instant.elapsed());
    instant = Instant::now();

    fs::create_dir_all(&conf::vals.output).unwrap();
    data.write(&conf::vals.output);

    debug!("dump `{:?}`", instant.elapsed());

    info!("finished `{}`", filename);
}

fn main() {
    let mut logd = fern::Dispatch::new()
        .format(|out, msg, _record| {
            out.finish(format_args!("{} - {}", chrono::Utc::now(), msg))
        })
        .level(log::LevelFilter::Debug)
        .chain(std::io::stderr());
    if let Ok(mut cexe) = std::env::current_exe() {
        cexe.set_extension("log");
        logd = logd.chain(fern::log_file(cexe).unwrap());
    }
    logd.apply().unwrap();

    let filemeta = fs::metadata(&conf::vals.file).unwrap();
    if filemeta.is_file() {
        perform(&conf::vals.file);
    } else if filemeta.is_dir() {
        for entry in fs::read_dir(&conf::vals.file).unwrap() {
            let entry = entry.unwrap();

            if let Ok(filemeta) = entry.metadata() {
                if filemeta.is_file() {
                    if let Some(ext) = entry.path().extension() {
                        if ext == "json" {
                            perform(entry.path().to_str().unwrap());
                        }
                    }
                }
            }
        }
    }
}
