extern crate serde_json;
extern crate string_interner;

use serde_json::{Map,Value};
use string_interner::{StringInterner,Sym};

use std::collections::HashMap;
use std::env;
use std::fmt;
use std::fs::File;
use std::io::{BufRead,BufReader};
use std::mem::size_of;
use std::thread;

#[derive(Debug)]
enum ValVec {
    InternedString(Vec<Sym>),
    Integer(Vec<i64>),
    Float(Vec<f64>),
}

#[derive(Debug)]
enum Val {
    InternedString(Sym),
    Integer(i64),
    Float(f64),
}

union MU {
    is: Sym,
    i: i64,
    f: f64,
}

union MU2TELLME {
    is: Option<Sym>,
    i: Option<i64>,
    f: Option<f64>,
}

impl fmt::Debug for MU {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        unsafe {
            match self {
                MU { is } => write!(f, "MU{{ is: {:?} }}", self.is),
                _ => write!(f, "MU{{ is: {:?}, i: {}, f: {} }}", self.is, self.i, self.f),
            }
        }
    }
}

impl ValVec {
    fn push_is(&mut self, value: Sym) {
        match self {
            ValVec::InternedString(i) => i.push(value),
            _ => (),
        }
    }
    fn push_i(&mut self, value: i64) {
        match self {
            ValVec::Integer(i) => i.push(value),
            _ => (),
        }
    }
    fn push_f(&mut self, value: f64) {
        match self {
            ValVec::Float(i) => i.push(value),
            _ => (),
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let file = File::open(&args[1]).unwrap();

    let mut si = StringInterner::default();
    // let mut data: HashMap<String, ValVec> = HashMap::new();
    let mut data: HashMap<String, Vec<MU>> = HashMap::new();

    let mu2 = MU2TELLME { i: Some(64) };

    for line in BufReader::new(file).lines() {
        let v: Map<String, Value> = serde_json::from_str(&line.unwrap()).unwrap();
        for (key, value) in v {
            match value {
                Value::Number(n) => {
                    if n.is_i64() {
                        data
                            .entry(key)
                            .or_default()
                            .push(MU{ i: n.as_i64().unwrap() });
                    } else if n.is_f64() {
                        data
                            .entry(key)
                            .or_default()
                            .push(MU{ f: n.as_f64().unwrap() });
                    }
                },
                Value::String(s) => {
                    data
                        .entry(key)
                        .or_default()
                        .push(MU{ is: si.get_or_intern(s) });
                },
                // Value::Number(n) => {
                //     if n.is_i64() {
                //         data
                //             .entry(key)
                //             .or_insert(ValVec::Integer(Vec::new()))
                //             .push_i(n.as_i64().unwrap());
                //     } else if n.is_f64() {
                //         data
                //             .entry(key)
                //             .or_insert(ValVec::Float(Vec::new()))
                //             .push_f(n.as_f64().unwrap());
                //     }
                // },
                // Value::String(s) => {
                //     data
                //         .entry(key)
                //         .or_insert(ValVec::InternedString(Vec::new()))
                //         .push_is(si.get_or_intern(s));
                // },
                _ => (),
            }
        }
    }
    if args.len() > 2 {
        thread::sleep_ms(60000);
    } else {
        println!("{:?}", data);
    }

    println!("mu2:{}", size_of::<MU2TELLME>());
    println!("val:{}", size_of::<Val>());
}
