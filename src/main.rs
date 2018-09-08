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
use std::time::Instant;

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

fn write_numeric_header(fo: &mut BufWriter<File>, meta: &str, length: u32) {
    fo.write_u32::<BigEndian>(meta.len() as u32).unwrap();
    fo.write(meta.as_bytes()).unwrap();

    fo.write_u8(2).unwrap(); // VERSION

    fo.write_u32::<BigEndian>(length).unwrap(); // totalSize

    fo.write_u32::<BigEndian>(8192).unwrap(); // sizePer
    fo.write_u8(0xff).unwrap(); // compression
    fo.write_u8(1).unwrap(); // VERSION
    fo.write_u8(0).unwrap(); // REVERSE_LOOKUP_DISALLOWED
    let values_size = length * 8 + 4; // [aka. headerOut] + Integer.BYTES
    fo.write_u32::<BigEndian>(values_size + 8).unwrap(); // + headerOut size
    fo.write_u32::<BigEndian>(1).unwrap(); // numWritten (seems it's always 1, unless
                                           // working with multiple "chunks", merged
                                           // together [which mode does not seem to be
                                           // used in practice])
    fo.write_u32::<BigEndian>(values_size).unwrap();
    fo.write_u32::<BigEndian>(0).unwrap(); // "nullness marker"
}

fn main() {
    let mut instant = Instant::now();

    let args: Vec<String> = env::args().collect();
    let file = File::open(&args[1]).unwrap();

    let mut si = StringInterner::default();
    let mut data: HashMap<String, ValVec> = HashMap::new();

    println!("init `{:?}`", instant.elapsed());
    instant = Instant::now();

    for line in BufReader::new(file).lines() {
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

    let mut rows = 0;
    if let ValVec::Integer(ts) = &data["timestamp"] {
        rows = ts.len();
    }

    data.insert("count".to_string(), ValVec::Integer(vec![1; rows]));

    println!("json `{:?}`", instant.elapsed());
    instant = Instant::now();

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

    println!("sort `{:?}`", instant.elapsed());
    instant = Instant::now();

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
            "bitmapSerdeFactory": {"type": "concise"},
            "byteOrder": "LITTLE_ENDIAN",
        }],
    });

    let mut meta_types = HashMap::new();
    meta_types.insert("long", meta_long.to_string());
    meta_types.insert("double", meta_double.to_string());
    meta_types.insert("string", meta_string.to_string());

    let keys = vec![
        "timestamp",
        "count",
        "vendor", "technology", "version", "ne_type", "object_id", "counter_id",
        "granularity", "end_timestamp", "export_timestamp", "processed_timestamp",
        "value_num",
    ];

    let fop = File::create("00000.smoosh").unwrap();
    let mut fo = BufWriter::new(fop);
    // XXX: We should be able to merge cols and dims with sth clever
    let mut cols_index_header = vec![];
    let mut cols_index_header_size = 0;
    let mut cols_index = vec![];
    let mut dims_index_header = vec![];
    let mut dims_index_header_size = 0;
    let mut dims_index = vec![];

    for key in keys {
        let datum = data.get_mut(key).unwrap();

        if key != "timestamp" {
            cols_index_header_size += 4 + key.len() as u32;
            cols_index_header.write_u32::<BigEndian>(cols_index_header_size).unwrap();
            cols_index.write_u32::<BigEndian>(0).unwrap();
            cols_index.write(key.as_bytes()).unwrap();

            if key != "count" {
                dims_index_header_size += 4 + key.len() as u32;
                dims_index_header.write_u32::<BigEndian>(dims_index_header_size).unwrap();
                dims_index.write_u32::<BigEndian>(0).unwrap();
                dims_index.write(key.as_bytes()).unwrap();
            }
        }

        match datum {
            ValVec::InternedString(is) => {
                fo.write_u32::<BigEndian>(meta_types["string"].len() as u32).unwrap();
                fo.write(meta_types["string"].as_bytes()).unwrap();

                fo.write_u16::<BigEndian>(1).unwrap();
                fo.write_u8(1).unwrap();

                fo.write_u32::<BigEndian>(14).unwrap(); // FIXME: This is not constant
                is.sort();
                is.dedup();

                fo.write_u32::<BigEndian>(is.len() as u32).unwrap();
                let mut offset = 0;
                for v in is {
                    let vv = si.resolve(*v).unwrap();
                    offset += vv.len() as u32 + 4;
                    fo.write_u32::<BigEndian>(offset).unwrap();
                    fo.write_u32::<BigEndian>(0).unwrap(); // Some kind of padding...?
                    fo.write(vv.as_bytes()).unwrap();
                }
            },
            ValVec::Integer(i) => {
                write_numeric_header(&mut fo, &meta_types["long"], i.len() as u32);

                for v in i {
                    fo.write_i64::<LittleEndian>(*v).unwrap();
                }
            },
            ValVec::Float(f) => {
                write_numeric_header(&mut fo, &meta_types["double"], f.len() as u32);

                for v in f {
                    fo.write_f64::<LittleEndian>(*v).unwrap();
                }
            },
        }
    }

    fo.write_u8(1).unwrap(); // GenericIndexed.VERSION_ONE
    fo.write_u8(0).unwrap(); // GenericIndexed.REVERSE_LOOKUP_DISALLOWED
    fo.write_u32::<BigEndian>(222).unwrap(); // byteBuffer.cap (FIXME: NOT CONST)
    fo.write_u32::<BigEndian>(data.len() as u32 - 1).unwrap(); // GenericIndexed.size (number of columns, without timestamp)
    fo.write(&cols_index_header).unwrap();
    fo.write(&cols_index).unwrap();
    fo.write_u8(1).unwrap(); // GenericIndexed.VERSION_ONE
    fo.write_u8(0).unwrap(); // GenericIndexed.REVERSE_LOOKUP_DISALLOWED
    fo.write_u32::<BigEndian>(209).unwrap(); // byteBuffer.cap (FIXME: NOT CONST)
    fo.write_u32::<BigEndian>(data.len() as u32 - 2).unwrap(); // GenericIndexed.size (number of dims, without timestamp and count)
    fo.write(&dims_index_header).unwrap();
    fo.write(&dims_index).unwrap();

    if let ValVec::Integer(ts) = &data["timestamp"] {
        fo.write_i64::<BigEndian>(ts[0]).unwrap();
        fo.write_i64::<BigEndian>(ts[ts.len() - 1] + 86400000).unwrap(); // XXX: DAY granularity
    }

    let bitmap_type = json!({
        "type": "concise",
    });
    let generic_meta = json!({
        "container": {},
        "aggregators": [{
            "type": "longSum",
            "name": "count",
            "fieldName": "count",
            "expression": Value::Null,
        }],
        "timestampSpec": {
            "column": "timestamp",
            "format": "millis",
            "missingValue": Value::Null,
        },
        "queryGranularity": {
            "type": "none",
        },
        "rollup": true,
    });

    fo.write_u32::<BigEndian>(18).unwrap();
    fo.write(bitmap_type.to_string().as_bytes()).unwrap();
    fo.write(generic_meta.to_string().as_bytes()).unwrap();

    println!("dump `{:?}`", instant.elapsed());


    if args.len() > 2 {
        thread::sleep_ms(60000);
    } else {
        println!("{:?}", data);
    }
}
