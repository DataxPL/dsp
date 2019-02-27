extern crate byteorder;
#[macro_use]
extern crate clap;
#[macro_use] extern crate lazy_static;
extern crate indexmap;
#[macro_use] extern crate log;
extern crate lz4;
extern crate num_cpus;
#[macro_use]
extern crate serde_json;
extern crate structopt;

use byteorder::{BE, LE, WriteBytesExt};
use indexmap::IndexSet;
use serde_json::Value;

use std::collections::HashMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

pub mod conf;
mod interner;
mod zip;
use interner::IS;
use zip::Zip;

lazy_static! {
    static ref META_TYPES: HashMap<&'static str, String> = {
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
        meta_types
    };
}

#[derive(Debug)]
enum ValVec {
    IndexedString(IS),
    Integer(Vec<i64>),
    Float(Vec<f64>),
}

trait VVWrite {
    fn write(&self, writer: &mut Write);
}

impl VVWrite for i64 {
    fn write(&self, writer: &mut Write) {
        writer.write_i64::<LE>(*self).unwrap();
    }
}

impl VVWrite for f64 {
    fn write(&self, writer: &mut Write) {
        writer.write_f64::<LE>(*self).unwrap();
    }
}

fn compress(out: &mut Write, data: &[u8]) {
    out.write_u32::<BE>(0).unwrap(); // "nullness marker"
    match conf::vals.compression {
        conf::Compression::None => out.write(&data).unwrap(),
        conf::Compression::LZ4 => out.write(&lz4::block::compress(
            &data,
            Some(lz4::block::CompressionMode::HIGHCOMPRESSION(9)),
            false,
        ).unwrap()).unwrap(),
    };
}

fn write_numeric<T: VVWrite>(writer: &mut Write, meta: &str, data: &[T]) {
    writer.write_u32::<BE>(meta.len() as u32).unwrap();
    writer.write_all(meta.as_bytes()).unwrap();

    writer.write_u8(2).unwrap(); // VERSION

    let length = data.len();

    writer.write_u32::<BE>(length as u32).unwrap(); // totalSize

    let size_per = 8192;
    writer.write_u32::<BE>(size_per as u32).unwrap();
    writer.write_u8(conf::vals.compression as u8).unwrap(); // compression
    writer.write_u8(1).unwrap(); // VERSION
    writer.write_u8(0).unwrap(); // REVERSE_LOOKUP_DISALLOWED

    let mut header = Vec::with_capacity(data.len() / size_per * 4);
    let mut values_all = Vec::with_capacity(data.len() * 8);
    let mut values_batch = Vec::with_capacity(size_per);

    for chunk in data.chunks(size_per) {
        for v in chunk {
            v.write(&mut values_batch);
        }
        compress(&mut values_all, &values_batch);
        values_batch.clear();
        header.write_u32::<BE>(values_all.len() as u32).unwrap();
    }

    writer.write_u32::<BE>((header.len() + values_all.len() + 4) as u32).unwrap(); // + Integer.NUM_BYTES
    writer.write_u32::<BE>((length as f64 / size_per as f64).ceil() as u32).unwrap(); // numWritten
    writer.write_all(&header).unwrap();
    writer.write_all(&values_all).unwrap();
}

impl ValVec {
    fn push_s(&mut self, value: String) {
        if let ValVec::IndexedString(is) = self { is.add_s(value) }
    }

    fn push_i(&mut self, value: i64) {
        if let ValVec::Integer(i) = self { i.push(value) }
    }

    fn push_f(&mut self, value: f64) {
        if let ValVec::Float(f) = self { f.push(value) }
    }

    fn append(&mut self, other: &mut ValVec) {
        match (self, other) {
            (ValVec::IndexedString(is), ValVec::IndexedString(o)) => is.append(o),
            (ValVec::Integer(i), ValVec::Integer(o)) => i.append(o),
            (ValVec::Float(f), ValVec::Float(o)) => f.append(o),
            (_, _) => unreachable!(),
        }
    }

    fn len(&self) -> usize {
        match self {
            ValVec::IndexedString(is) => is.len(),
            ValVec::Integer(i) => i.len(),
            ValVec::Float(f) => f.len(),
        }
    }
}

#[derive(Debug, Default)]
pub struct Data(HashMap<String, ValVec>);

impl Data {
    pub fn new() -> Self {
        Data(HashMap::new())
    }

    pub fn add_s(&mut self, key: String, value: String) {
        self.0.entry(key)
            .or_insert_with(|| ValVec::IndexedString(IS::new()))
            .push_s(value);
    }

    pub fn add_i(&mut self, key: String, value: i64) {
        self.0.entry(key)
            .or_insert_with(|| ValVec::Integer(Vec::new()))
            .push_i(value);
    }

    pub fn add_f(&mut self, key: String, value: f64) {
        self.0.entry(key)
            .or_insert_with(|| ValVec::Float(Vec::new()))
            .push_f(value);
    }

    pub fn append(&mut self, other: Data) {
        for (key, mut value) in other.0 {
            self.0.entry(key)
                .and_modify(|e| e.append(&mut value))
                .or_insert(value);
        }
    }

    fn rows(&self) -> usize {
        match self.0.values().next() {
            Some(val) => val.len(),
            None => 0,
        }
    }

    pub fn preaggregate(&mut self) {
        let rows = self.rows();
        self.0.insert("count".to_string(), ValVec::Integer(vec![1; rows]));
    }

    pub fn sort(&mut self) {
        let mut perm: Vec<usize> = Vec::new();
        if let ValVec::Integer(ts) = &self.0["timestamp"] {
            perm = (0..ts.len()).collect();
            perm.sort_unstable_by_key(|&i| &ts[i]);
        }
        let mut new0 = HashMap::new();
        for (k, v) in self.0.drain() {
            match v {
                ValVec::IndexedString(mut is) => {
                    is.sort_and_permute(&perm);
                    new0.insert(k, ValVec::IndexedString(is));
                },
                ValVec::Integer(i) => {
                    new0.insert(k, ValVec::Integer(perm.iter().map(|p| i[*p]).collect()));
                },
                ValVec::Float(f) => {
                    new0.insert(k, ValVec::Float(perm.iter().map(|p| f[*p]).collect()));
                },
            }
        }
        self.0 = new0;
    }

    pub fn write(&self, path: &PathBuf) {
        if let Some(compression) = conf::vals.zip {
            let mut version = vec![];
            let mut factory = vec![];
            let mut data = vec![];
            let mut meta = vec![];
            self.write_version(&mut version);
            self.write_factory(&mut factory);
            self.write_data(&mut data, &mut meta);

            let instant = Instant::now();

            let zip = Zip::new(path.join("index.zip"), compression).unwrap();
            zip.file_add("version.bin", &version).unwrap();
            zip.file_add("factory.json", &factory).unwrap();
            zip.file_add("00000.smoosh", &data).unwrap();
            zip.file_add("meta.smoosh", &meta).unwrap();
            zip.close().unwrap();

            debug!("zipf `{:?}`", instant.elapsed());

            return;
        }
        let mut file = fs::File::create(path.join("version.bin")).unwrap();
        self.write_version(&mut file);
        file = fs::File::create(path.join("factory.json")).unwrap();
        self.write_factory(&mut file);
        file = fs::File::create(path.join("00000.smoosh")).unwrap();
        let mut meta_file = fs::File::create(path.join("meta.smoosh")).unwrap();
        self.write_data(&mut file, &mut meta_file);
    }

    fn write_version(&self, writer: &mut Write) {
        writer.write_u32::<BE>(9).unwrap();
    }

    fn write_factory(&self, writer: &mut Write) {
        let factory = json!({"type": "mMapSegmentFactory"});
        serde_json::to_writer(writer, &factory).unwrap();
    }

    fn write_data(&self, data_writer: &mut Write, meta_writer: &mut Write) {
        let mut writer = BufWriter::new(data_writer);
        let mut m_writer = BufWriter::new(meta_writer);

        let metrics = conf::vals.metrics.iter().collect::<IndexSet<_>>();
        let mut dimensions = conf::vals.dimensions.iter().collect::<IndexSet<_>>();
        if dimensions.is_empty() {
            for dimension in self.0.keys() {
                dimensions.insert(dimension);
            }
        }
        dimensions = &dimensions - &metrics;

        let mut offset = self.write_key(&mut writer, "timestamp");
        write!(&mut m_writer, "v1,{},1\n__time,0,0,{}\n", std::i32::MAX, offset).unwrap();

        let mut cols_index = Vec::with_capacity((dimensions.len() + metrics.len()) * 4);
        let mut cols_index_header = Vec::with_capacity((dimensions.len() + metrics.len()) * 4);
        let mut dims_index_header = Vec::with_capacity(dimensions.len() * 4);

        for key in metrics {
            cols_index.write_u32::<BE>(0).unwrap();
            cols_index.write_all(key.as_bytes()).unwrap();
            cols_index_header.write_u32::<BE>(cols_index.len() as u32).unwrap();

            let next_offset = offset + self.write_key(&mut writer, key);
            write!(&mut m_writer, "{},0,{},{}\n", key, offset, next_offset).unwrap();
            offset = next_offset;
        }
        let mut metas = IndexSet::new();
        let cols_index_offset = cols_index.len();
        for key in dimensions {
            cols_index.write_u32::<BE>(0).unwrap();
            cols_index.write_all(key.as_bytes()).unwrap();
            cols_index_header.write_u32::<BE>(cols_index.len() as u32).unwrap();
            dims_index_header.write_u32::<BE>(cols_index[cols_index_offset..].len() as u32).unwrap();

            let next_offset = offset + self.write_key(&mut writer, key);
            metas.insert(format!("{},0,{},{}\n", key, offset, next_offset));
            offset = next_offset;
        }

        self.write_columns_index(&mut writer, &cols_index, &cols_index_header, 1);
        self.write_columns_index(&mut writer, &cols_index[cols_index_offset..], &dims_index_header, 2);

        if let ValVec::Integer(ts) = &self.0["timestamp"] {
            writer.write_i64::<BE>(ts[0]).unwrap();
            writer.write_i64::<BE>(ts[0] + 86_400_000).unwrap(); // XXX: DAY granularity
        }

        let bitmap_type = json!({
            "type": "concise",
        }).to_string();
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
        }).to_string();

        let new_offset =
            offset +
            cols_index.len() +
            cols_index_header.len() +
            dims_index_header.len() +
            (cols_index.len() - cols_index_offset) +
            20 + // index metadata (from `write_columns_index`, 1+1+4+4)
            16 + // segment boundaries (from `if "timestamp"` above, 8+8)
            4 + // `bitmap_type` length field (`18` below)
            bitmap_type.len()
        ;
        metas.insert(format!("index.drd,0,{},{}\n", offset, new_offset));
        metas.insert(format!("metadata.drd,0,{},{}\n", new_offset, new_offset + generic_meta.len()));
        metas.sort();
        for meta in metas {
            m_writer.write_all(meta.as_bytes()).unwrap();
        }

        writer.write_u32::<BE>(18).unwrap();
        writer.write_all(bitmap_type.as_bytes()).unwrap();
        writer.write_all(generic_meta.as_bytes()).unwrap();
    }

    fn write_key(&self, writer: &mut Write, key: &str) -> usize {
        let mut column = vec![];
        match &self.0[key] {
            ValVec::IndexedString(is) => {
                column.write_u32::<BE>(META_TYPES["string"].len() as u32).unwrap();
                column.write_all(META_TYPES["string"].as_bytes()).unwrap();

                is.write(&mut column);
            },
            ValVec::Integer(i) => write_numeric(&mut column, &META_TYPES["long"], i),
            ValVec::Float(f) => write_numeric(&mut column, &META_TYPES["double"], f),
        }
        writer.write_all(&column).unwrap();
        column.len()
    }

    fn write_columns_index(&self, writer: &mut Write, index: &[u8], header: &[u8], delta: u32) {
        writer.write_u8(1).unwrap(); // GenericIndexed.VERSION_ONE
        writer.write_u8(0).unwrap(); // GenericIndexed.REVERSE_LOOKUP_DISALLOWED
        writer.write_u32::<BE>((header.len() + index.len() + 4) as u32).unwrap(); // + Integer.BYTES
        writer.write_u32::<BE>(self.0.len() as u32 - delta).unwrap(); // GenericIndexed.size (number of columns/dimensions, without timestamp)
        writer.write_all(&header).unwrap();
        writer.write_all(&index).unwrap();
    }
}
