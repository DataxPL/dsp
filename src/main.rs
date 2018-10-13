extern crate byteorder;
extern crate concise;
extern crate crossbeam_channel;
extern crate indexmap;
extern crate itertools;
#[macro_use]
extern crate serde_json;
extern crate string_cache;

use byteorder::{BigEndian,LittleEndian,WriteBytesExt};
use concise::CONCISE;
use indexmap::IndexMap;
use itertools::Itertools;
use serde_json::{Map,Value};
use string_cache::DefaultAtom as Atom;

use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufRead,BufReader,BufWriter,Write};
// use std::mem::size_of;
use std::thread;
use std::time::Instant;

extern crate ds;
use ds::ValVec;

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

    let mut data: HashMap<String, ValVec> = HashMap::new();

    println!("init `{:?}`", instant.elapsed());
    instant = Instant::now();

    let N = 16;

    let (tx_ch, rx_ch): (
        crossbeam_channel::Sender<Vec<String>>,
        crossbeam_channel::Receiver<Vec<String>>
    ) = crossbeam_channel::bounded(N * 4);
    let (tx_res, rx_res) = crossbeam_channel::unbounded();

    for _ in 0..N {
        let rx_ch = rx_ch.clone();
        let tx_res = tx_res.clone();
        thread::spawn(move || {
            let mut data: HashMap<String, ValVec> = HashMap::new();
            for chunk in rx_ch {
                for line in chunk {
                    let v: Map<String, Value> = serde_json::from_str(&line).unwrap();
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
                                    .push_is(Atom::from(s));
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

    for chunk_iter in &BufReader::new(file).lines().chunks(2) {
        let chunk: Vec<String> = chunk_iter.map(|c| c.unwrap()).collect();
        tx_ch.send(chunk);
    }
    drop(tx_ch);

    for part in rx_res {
        for (key, mut value) in part {
            if !data.contains_key(&key) {
                data.insert(key, value);
            } else {
                data.get_mut(&key).unwrap().append(&mut value);
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
    // (usize has no negative values, but we want to have them in perm)
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

                fo.write_u8(0).unwrap(); // VERSION (UNCOMPRESSED_SINGLE_VALUE)
                fo.write_u8(1).unwrap(); // VERSION_ONE
                fo.write_u8(1).unwrap(); // REVERSE_LOOKUP_ALLOWED

                // XXX: Maybe it's better idea to store str lengths at data
                // creation time and avoid these buffers?
                let mut index_header = vec![];
                let mut index_items = vec![];
                let mut index_values = vec![];

                let mut bitmap_header = vec![];
                let mut bitmap_values = vec![];

                let mut map = IndexMap::new();
                for (i, v) in is.iter().enumerate() {
                    map.entry(v).or_insert(Vec::new()).push(i);
                }
                map.sort_keys();

                index_values.write_u8(0).unwrap(); // VERSION
                index_values.write_u8(1).unwrap(); // numBytes
                index_values.write_u32::<BigEndian>(is.len() as u32 + 3).unwrap(); // + padding
                let vl = index_values.len(); // This has to be separate, to please borrow checker
                index_values.resize(vl + is.len(), 0);

                let mut offset = 0;
                for (i, (k, v)) in map.iter().enumerate() {
                    offset += k.len() as u32 + 4; // + "nullness marker"
                    index_header.write_u32::<BigEndian>(offset).unwrap();
                    index_items.write_u32::<BigEndian>(0).unwrap(); // "nullness marker"

                    bitmap_header.write_u32::<BigEndian>((i as u32 + 1) * 8).unwrap();

                    let mut concise = CONCISE::new();

                    index_items.write(k.as_bytes()).unwrap();
                    for vv in v {
                        index_values[6 + vv] = i as u8;
                        concise.append(*vv as i32);
                    }

                    bitmap_values.write_u32::<BigEndian>(0).unwrap();
                    // TODO: More than one byte of a bitmap
                    bitmap_values.write_i32::<BigEndian>(concise.words.unwrap()[0].0).unwrap();
                }

                index_values.write(&[0, 0, 0]).unwrap(); // padding

                fo.write_u32::<BigEndian>(
                    index_header.len() as u32 + index_items.len() as u32 + 4
                ).unwrap(); // + Integer.BYTES
                fo.write_u32::<BigEndian>(map.len() as u32).unwrap(); // numWritten
                fo.write(&index_header).unwrap();
                fo.write(&index_items).unwrap();
                fo.write(&index_values).unwrap();

                fo.write_u8(1).unwrap(); // VERSION
                fo.write_u8(0).unwrap(); // REVERSE_LOOKUP_DISALLOWED
                let maplen = map.len() as u32;
                // Another header + values + 4 sizing
                fo.write_u32::<BigEndian>(maplen * 4 + maplen * 8 + 4).unwrap();
                fo.write_u32::<BigEndian>(map.len() as u32).unwrap();

                fo.write(&bitmap_header).unwrap();
                fo.write(&bitmap_values).unwrap();
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
    fo.write_u32::<BigEndian>(
        (cols_index_header.len() + cols_index.len() + 4) as u32
    ).unwrap(); // + Integer.BYTES
    fo.write_u32::<BigEndian>(data.len() as u32 - 1).unwrap(); // GenericIndexed.size (number of columns, without timestamp)
    fo.write(&cols_index_header).unwrap();
    fo.write(&cols_index).unwrap();
    fo.write_u8(1).unwrap(); // GenericIndexed.VERSION_ONE
    fo.write_u8(0).unwrap(); // GenericIndexed.REVERSE_LOOKUP_DISALLOWED
    fo.write_u32::<BigEndian>(
        (dims_index_header.len() + dims_index.len() + 4) as u32
    ).unwrap(); // + Integer.BYTES
    fo.write_u32::<BigEndian>(data.len() as u32 - 2).unwrap(); // GenericIndexed.size (number of dims, without timestamp and count)
    fo.write(&dims_index_header).unwrap();
    fo.write(&dims_index).unwrap();

    if let ValVec::Integer(ts) = &data["timestamp"] {
        fo.write_i64::<BigEndian>(ts[0]).unwrap();
        fo.write_i64::<BigEndian>(ts[0] + 86400000).unwrap(); // XXX: DAY granularity
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
        if args.len() > 3 {
            thread::sleep_ms(60000);
        }
    } else {
        println!("{:?}", data);
    }
}
