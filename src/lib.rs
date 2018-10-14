extern crate byteorder;
extern crate concise;
extern crate indexmap;
#[macro_use]
extern crate serde_json;
extern crate string_cache;

use byteorder::{BigEndian,LittleEndian,WriteBytesExt};
use concise::CONCISE;
use indexmap::IndexMap;
use serde_json::Value;
use string_cache::DefaultAtom as Atom;

use std::collections::HashMap;
use std::io::Write;
use std::ops::{Deref, DerefMut};

fn write_numeric_header(writer: &mut Write, meta: &str, length: u32) {
    writer.write_u32::<BigEndian>(meta.len() as u32).unwrap();
    writer.write(meta.as_bytes()).unwrap();

    writer.write_u8(2).unwrap(); // VERSION

    writer.write_u32::<BigEndian>(length).unwrap(); // totalSize

    writer.write_u32::<BigEndian>(8192).unwrap(); // sizePer
    writer.write_u8(0xff).unwrap(); // compression
    writer.write_u8(1).unwrap(); // VERSION
    writer.write_u8(0).unwrap(); // REVERSE_LOOKUP_DISALLOWED
    let values_size = length * 8 + 4; // [aka. headerOut] + Integer.BYTES
    writer.write_u32::<BigEndian>(values_size + 8).unwrap(); // + headerOut size
    writer.write_u32::<BigEndian>(1).unwrap(); // numWritten (seems it's always 1, unless
                                           // working with multiple "chunks", merged
                                           // together [which mode does not seem to be
                                           // used in practice])
    writer.write_u32::<BigEndian>(values_size).unwrap();
    writer.write_u32::<BigEndian>(0).unwrap(); // "nullness marker"
}

#[derive(Debug)]
pub enum ValVec {
    InternedString(Vec<Atom>),
    Integer(Vec<i64>),
    Float(Vec<f64>),
}

#[macro_export]
macro_rules! valvec_call {
    ($self:ident, $fn:ident, $v:expr) => (
        match $self {
            ValVec::InternedString(is) => is.$fn($v as Sym),
            ValVec::Integer(i) => i.$fn($v),
            ValVec::Float(f) => f.$fn($v),
        }
    )
}

impl ValVec {
    // pub fn push2<T>(&mut self, value: T) {
    //     valvec_call!(self, push, value);
    // }
    pub fn push_is(&mut self, value: Atom) {
        match self {
            ValVec::InternedString(i) => i.push(value),
            _ => (),
        }
    }
    pub fn push_i(&mut self, value: i64) {
        match self {
            ValVec::Integer(i) => i.push(value),
            _ => (),
        }
    }
    pub fn push_f(&mut self, value: f64) {
        match self {
            ValVec::Float(i) => i.push(value),
            _ => (),
        }
    }

    pub fn append(&mut self, other: &mut ValVec) {
        match (self, other) {
            (ValVec::InternedString(is), ValVec::InternedString(o)) => is.append(o),
            (ValVec::Integer(i), ValVec::Integer(o)) => i.append(o),
            (ValVec::Float(f), ValVec::Float(o)) => f.append(o),
            (_, _) => (), // XXX: panic?
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ValVec::InternedString(s) => s.len(),
            ValVec::Integer(i) => i.len(),
            ValVec::Float(f) => f.len(),
        }
    }
}

#[derive(Debug)]
pub struct Data(HashMap<String, ValVec>);

impl Deref for Data {
    type Target = HashMap<String, ValVec>;

    fn deref(&self) -> &HashMap<String, ValVec> {
        &self.0
    }
}

impl DerefMut for Data {
    fn deref_mut(&mut self) -> &mut HashMap<String, ValVec> {
        &mut self.0
    }
}

impl Data {
    pub fn new() -> Data {
        return Data(HashMap::new());
    }

    pub fn add_s(&mut self, key: String, value: Atom) {
        self.0.entry(key).or_insert(ValVec::InternedString(Vec::new())).push_is(value);
    }

    pub fn add_i(&mut self, key: String, value: i64) {
        self.0.entry(key).or_insert(ValVec::Integer(Vec::new())).push_i(value);
    }

    pub fn add_f(&mut self, key: String, value: f64) {
        self.0.entry(key).or_insert(ValVec::Float(Vec::new())).push_f(value);
    }

    pub fn append(&mut self, other: Data) {
        for (key, mut value) in other.0 {
            if !self.0.contains_key(&key) {
                self.0.insert(key, value);
            } else {
                self.0.get_mut(&key).unwrap().append(&mut value);
            }
        }
    }

    pub fn rows(&self) -> usize {
        match self.0.values().next() {
            Some(val) => val.len(),
            None => 0,
        }
    }

    pub fn sort(&mut self) {
        // FIXME: Deal with this i64 vs. usize thing
        // (usize has no negative values, but we want to have them in perm)
        let mut perm: Vec<i64> = Vec::new();
        if let ValVec::Integer(ts) = &self.0["timestamp"] {
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
                for val in self.0.values_mut() {
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
    }

    pub fn write(&self, writer: &mut Write) {
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

        // XXX: We should be able to merge cols and dims with sth clever
        let mut cols_index_header = vec![];
        let mut cols_index_header_size = 0;
        let mut cols_index = vec![];
        let mut dims_index_header = vec![];
        let mut dims_index_header_size = 0;
        let mut dims_index = vec![];

        for key in keys {
            let datum = self.0.get(key).unwrap();

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
                    writer.write_u32::<BigEndian>(meta_types["string"].len() as u32).unwrap();
                    writer.write(meta_types["string"].as_bytes()).unwrap();

                    writer.write_u8(0).unwrap(); // VERSION (UNCOMPRESSED_SINGLE_VALUE)
                    writer.write_u8(1).unwrap(); // VERSION_ONE
                    writer.write_u8(1).unwrap(); // REVERSE_LOOKUP_ALLOWED

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

                    writer.write_u32::<BigEndian>(
                        index_header.len() as u32 + index_items.len() as u32 + 4
                    ).unwrap(); // + Integer.BYTES
                    writer.write_u32::<BigEndian>(map.len() as u32).unwrap(); // numWritten
                    writer.write(&index_header).unwrap();
                    writer.write(&index_items).unwrap();
                    writer.write(&index_values).unwrap();

                    writer.write_u8(1).unwrap(); // VERSION
                    writer.write_u8(0).unwrap(); // REVERSE_LOOKUP_DISALLOWED
                    let maplen = map.len() as u32;
                    // Another header + values + 4 sizing
                    writer.write_u32::<BigEndian>(maplen * 4 + maplen * 8 + 4).unwrap();
                    writer.write_u32::<BigEndian>(map.len() as u32).unwrap();

                    writer.write(&bitmap_header).unwrap();
                    writer.write(&bitmap_values).unwrap();
                },
                ValVec::Integer(i) => {
                    write_numeric_header(writer, &meta_types["long"], i.len() as u32);

                    for v in i {
                        writer.write_i64::<LittleEndian>(*v).unwrap();
                    }
                },
                ValVec::Float(f) => {
                    write_numeric_header(writer, &meta_types["double"], f.len() as u32);

                    for v in f {
                        writer.write_f64::<LittleEndian>(*v).unwrap();
                    }
                },
            }
        }

        writer.write_u8(1).unwrap(); // GenericIndexed.VERSION_ONE
        writer.write_u8(0).unwrap(); // GenericIndexed.REVERSE_LOOKUP_DISALLOWED
        writer.write_u32::<BigEndian>(
            (cols_index_header.len() + cols_index.len() + 4) as u32
        ).unwrap(); // + Integer.BYTES
        writer.write_u32::<BigEndian>(self.0.len() as u32 - 1).unwrap(); // GenericIndexed.size (number of columns, without timestamp)
        writer.write(&cols_index_header).unwrap();
        writer.write(&cols_index).unwrap();
        writer.write_u8(1).unwrap(); // GenericIndexed.VERSION_ONE
        writer.write_u8(0).unwrap(); // GenericIndexed.REVERSE_LOOKUP_DISALLOWED
        writer.write_u32::<BigEndian>(
            (dims_index_header.len() + dims_index.len() + 4) as u32
        ).unwrap(); // + Integer.BYTES
        writer.write_u32::<BigEndian>(self.0.len() as u32 - 2).unwrap(); // GenericIndexed.size (number of dims, without timestamp and count)
        writer.write(&dims_index_header).unwrap();
        writer.write(&dims_index).unwrap();

        if let ValVec::Integer(ts) = &self.0["timestamp"] {
            writer.write_i64::<BigEndian>(ts[0]).unwrap();
            writer.write_i64::<BigEndian>(ts[0] + 86400000).unwrap(); // XXX: DAY granularity
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

        writer.write_u32::<BigEndian>(18).unwrap();
        writer.write(bitmap_type.to_string().as_bytes()).unwrap();
        writer.write(generic_meta.to_string().as_bytes()).unwrap();
    }
}
