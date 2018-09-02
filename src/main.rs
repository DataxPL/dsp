extern crate byteorder;
#[macro_use]
extern crate serde_json;
extern crate string_interner;

use byteorder::{BigEndian,LittleEndian,WriteBytesExt};
use serde_json::{Map,Value};
use string_interner::{StringInterner,Sym};

use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufRead,BufReader,BufWriter,Write};
// use std::mem::size_of;
use std::thread;

#[derive(Debug)]
enum ValVec {
    InternedString(Vec<Sym>),
    Integer(Vec<i64>),
    Float(Vec<f64>),
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
    let mut data: HashMap<String, ValVec> = HashMap::new();
    let mut rows = 0; // TODO: Quite possibly we could get rid of this

    for line in BufReader::new(file).lines() {
        rows += 1;
        let v: Map<String, Value> = serde_json::from_str(&line.unwrap()).unwrap();
        for (key, value) in v {
            match value {
                Value::Number(n) => {
                    if n.is_i64() {
                        data
                            .entry(key)
                            .or_insert(ValVec::Integer(Vec::new()))
                            .push_i(n.as_i64().unwrap());
                    } else if n.is_f64() {
                        data
                            .entry(key)
                            .or_insert(ValVec::Float(Vec::new()))
                            .push_f(n.as_f64().unwrap());
                    }
                },
                Value::String(s) => {
                    data
                        .entry(key)
                        .or_insert(ValVec::InternedString(Vec::new()))
                        .push_is(si.get_or_intern(s));
                },
                _ => (),
            }
        }
    }
    data.insert("count".to_string(), ValVec::Integer(vec![1; rows]));

    // FIXME: Deal with this i64 vs. usize thing
    let mut perm: Vec<i64> = Vec::new();
    if let ValVec::Integer(ts) = &data["timestamp"] {
        perm = (0..ts.len() as i64).collect();
        perm.sort_unstable_by_key(|&i| &ts[i as usize]);
    }
    for idx in 0..perm.len() {
        if perm[idx] < 0 {
            continue;
        }
        let mut curr_pos = idx;
        while perm[curr_pos] as usize != idx {
            let dest_pos = perm[curr_pos] as usize;
            for val in data.values_mut() {
                match val {
                    ValVec::InternedString(is) => is.swap(curr_pos, dest_pos),
                    ValVec::Integer(i) => i.swap(curr_pos, dest_pos),
                    ValVec::Float(f) => f.swap(curr_pos, dest_pos),
                }
            }
            perm[curr_pos] = -1 - dest_pos as i64;
            curr_pos = dest_pos;
        }
        perm[curr_pos] = -1 - perm[curr_pos];
    }

    let meta_long = json!({
        "valueType": "LONG",
        "hasMultipleValues": false,
        "parts": [{
            "type": "long",
            "byteOrder": "LITTLE_ENDIAN",
        }],
    });
    let meta_double = json!({
        "valueType": "DOUBLE",
        "hasMultipleValues": false,
        "parts": [{
            "type": "double",
            "byteOrder": "LITTLE_ENDIAN",
        }],
    });
    let meta_string = json!({
        "valueType": "STRING",
        "hasMultipleValues": false,
        "parts": [{
            "type": "stringDictionary",
            "byteOrder": "LITTLE_ENDIAN",
            "bitmapSerdeFactory": {"type": "concise"},
        }],
    });

    let mut meta_types = HashMap::new();
    meta_types.insert("long", meta_long.to_string());
    meta_types.insert("double", meta_double.to_string());
    meta_types.insert("string", meta_string.to_string());

    let keys = vec![
        "timestamp", "count", "vendor", "technology", "version",
        "ne_type", "object_id", "value_num",
    ];

    let fop = File::create("00000.smoosh").unwrap();
    let mut fo = BufWriter::new(fop);

    for key in keys {
        let data = &data[key];

        match data {
            ValVec::InternedString(is) => {
                fo.write_u32::<BigEndian>(meta_types["string"].len() as u32).unwrap();
                fo.write(meta_types["string"].as_bytes()).unwrap();
            },
            ValVec::Integer(i) => {
                fo.write_u32::<BigEndian>(meta_types["long"].len() as u32).unwrap();
                fo.write(meta_types["long"].as_bytes()).unwrap();

                fo.write_u8(2).unwrap();

                fo.write_u32::<BigEndian>(i.len() as u32).unwrap();

                fo.write(&[0, 0, 0x20, 0, 0xff, 1, 0]).unwrap();
                let magic1 = 12 + i.len() as u32 * 8;
                fo.write_u32::<BigEndian>(magic1).unwrap();
                fo.write_u32::<BigEndian>(1).unwrap();
                fo.write_u32::<BigEndian>(magic1 - 8).unwrap();
                fo.write_u32::<BigEndian>(0).unwrap();

                for v in i {
                    fo.write_i64::<LittleEndian>(*v).unwrap();
                }
            },
            ValVec::Float(f) => {
                fo.write_u32::<BigEndian>(meta_types["double"].len() as u32).unwrap();
                fo.write(meta_types["double"].as_bytes()).unwrap();

                fo.write_u8(2).unwrap();

                fo.write_u32::<BigEndian>(f.len() as u32).unwrap();

                fo.write(&[0, 0, 0x20, 0, 0xff, 1, 0]).unwrap();
                let magic1 = 12 + f.len() as u32 * 8;
                fo.write_u32::<BigEndian>(magic1).unwrap();
                fo.write_u32::<BigEndian>(1).unwrap();
                fo.write_u32::<BigEndian>(magic1 - 8).unwrap();
                fo.write_u32::<BigEndian>(0).unwrap();

                for v in f {
                    fo.write_f64::<LittleEndian>(*v).unwrap();
                }
            },
        }
    }


    if args.len() > 2 {
        thread::sleep_ms(60000);
    } else {
        println!("{:?}", data);
    }
}
