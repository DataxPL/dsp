extern crate crossbeam_channel;
extern crate itertools;
extern crate serde_json;

use itertools::Itertools;
use serde_json::{Map,Value};

use std::fs::File;
use std::io::{BufRead,BufReader,BufWriter};
// use std::mem::size_of;
use std::thread;
use std::time::Instant;

extern crate ds;
use ds::{Data, conf};

fn main() {
    let mut instant = Instant::now();

    let file = File::open(&conf::vals.file).unwrap();

    let mut data = Data::new();

    println!("init `{:?}`", instant.elapsed());
    instant = Instant::now();

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
            tx_res.send(data);
            drop(tx_res);
        });
    }

    drop(rx_ch);
    drop(tx_res);

    for chunk_iter in &BufReader::new(file).lines().chunks(1000) {
        let chunk: Vec<String> = chunk_iter.map(|c| c.unwrap()).collect();
        tx_ch.send(chunk);
    }
    drop(tx_ch);

    for part in rx_res {
        data.append(part);
    }

    data.preaggregate();

    println!("json `{:?}`", instant.elapsed());
    instant = Instant::now();

    data.sort();

    println!("sort `{:?}`", instant.elapsed());
    instant = Instant::now();

    let fop = File::create("22222.smoosh").unwrap();
    let mut fo = BufWriter::new(fop);

    data.write(&mut fo);

    println!("dump `{:?}`", instant.elapsed());


    // if args.len() > 2 {
    //     if args.len() > 3 {
    //         thread::sleep_ms(60000);
    //     }
    // } else {
    //     println!("{:?}", data);
    // }
}
