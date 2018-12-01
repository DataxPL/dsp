extern crate byteorder;
extern crate concise;
extern crate indexmap;

use byteorder::{BE, LE, WriteBytesExt};
use self::concise::CONCISE;
use self::indexmap::IndexSet;

use std::io::Write;

use conf;
use compress;

struct VInt {
    size: usize,
}

impl VInt {
    fn new(len: usize) -> Self {
        Self{
            size: ((len as f64).log2() / 8. + 1.) as usize,
        }
    }

    fn write_value(&self, out: &mut Write, val: usize) {
        match self.size {
            1 => out.write_u8(val as u8).unwrap(),
            2 => match conf::vals.compression {
                conf::Compression::None => out.write_u16::<BE>(val as u16).unwrap(),
                conf::Compression::LZ4 => out.write_u16::<LE>(val as u16).unwrap(),
            },
            3 => match conf::vals.compression {
                conf::Compression::None => out.write_u24::<BE>(val as u32).unwrap(),
                conf::Compression::LZ4 => out.write_u24::<LE>(val as u32).unwrap(),
            },
            4 => match conf::vals.compression {
                conf::Compression::None => out.write_u32::<BE>(val as u32).unwrap(),
                conf::Compression::LZ4 => out.write_u32::<LE>(val as u32).unwrap(),
            },
            _ => (),
        }
    }
}

#[derive(Debug)]
pub struct IS(Vec<ISF>);

impl IS {
    pub fn new() -> Self {
        IS(vec![ISF::new()])
    }

    pub fn add_s(&mut self, s: String) {
        self.0[0].add_s(s);
    }

    pub fn append(&mut self, other: &mut IS) {
        self.0.push(other.0.swap_remove(0));
    }

    pub fn len(&self) -> usize {
        self.0.iter().map(|isf| isf.len()).sum()
    }

    pub fn write(&self, writer: &mut Write) {
        let data = &self.0[0];

        let mut index_header = Vec::with_capacity(data.keys.len() * 4);
        let mut index_items = Vec::with_capacity(data.keys.len() * 4);
        for k in &data.keys {
            index_items.write_u32::<BE>(0).unwrap(); // nullness marker
            index_items.write_all(k.as_bytes()).unwrap();
            index_header.write_u32::<BE>(index_items.len() as u32).unwrap();
        }

        let vint = VInt::new(data.keys.len());

        let mut index_values = Vec::with_capacity(data.len() * vint.size);
        let mut bitmaps = vec![CONCISE::new(); data.keys.len()];
        for (v, i) in data.indexes.iter().enumerate() {
            vint.write_value(&mut index_values, *i);
            bitmaps[*i].append(v as i32);
        }

        let mut bitmap_header = Vec::with_capacity(bitmaps.len() * 4);
        let mut bitmap_values = Vec::with_capacity(bitmaps.len() * 4);
        bitmap_header.write_u32::<BE>(data.keys.len() as u32).unwrap();
        for bitmap in bitmaps {
            bitmap_values.write_i32::<BE>(0).unwrap();
            for word in bitmap.words_view() {
                bitmap_values.write_i32::<BE>(word.0).unwrap();
            }
            bitmap_header.write_u32::<BE>(bitmap_values.len() as u32).unwrap();
        }

        match conf::vals.compression {
            conf::Compression::None => {
                let num_padding = vec![0; 4 - vint.size];

                writer.write_u8(0).unwrap(); // VERSION (UNCOMPRESSED_SINGLE_VALUE)
                writer.write_u8(1).unwrap(); // VERSION_ONE
                writer.write_u8(1).unwrap(); // REVERSE_LOOKUP_ALLOWED
                writer.write_u32::<BE>(
                    index_header.len() as u32 + index_items.len() as u32 + 4
                ).unwrap(); // + Integer.BYTES
                writer.write_u32::<BE>(data.keys.len() as u32).unwrap(); // numWritten
                writer.write_all(&index_header).unwrap();
                writer.write_all(&index_items).unwrap();
                writer.write_u8(0).unwrap(); // VERSION
                writer.write_u8(vint.size as u8).unwrap(); // numBytes
                writer.write_u32::<BE>(
                    (data.len() * vint.size + num_padding.len()) as u32,
                ).unwrap();
                writer.write_all(&index_values).unwrap();
                writer.write_all(&num_padding).unwrap();
            },
            conf::Compression::LZ4 => {
                let chunk_factor = 65536 / 2_usize.pow((vint.size - 1) as u32);

                let mut index_header_c = Vec::with_capacity(index_values.len() * 4);
                let mut index_values_c = Vec::with_capacity(index_values.len());
                // This is written to header, so that we get proper size later on.
                index_header_c.write_u32::<BE>(
                    (data.len() as f64 / chunk_factor as f64).ceil() as u32,
                ).unwrap();
                for chunk in index_values.chunks(65536) {
                    compress(&mut index_values_c, chunk);
                    index_header_c.write_u32::<BE>(index_values_c.len() as u32).unwrap();
                }

                writer.write_u8(2).unwrap(); // VERSION (COMPRESSED)
                writer.write_u32::<BE>(0).unwrap(); // flags (stores info about bitmap/multivalue,
                                                    // our desired one turns out to be `0`).
                writer.write_u8(1).unwrap(); // VERSION_ONE
                writer.write_u8(1).unwrap(); // REVERSE_LOOKUP_ALLOWED
                writer.write_u32::<BE>(
                    index_header.len() as u32 + index_items.len() as u32 + 4
                ).unwrap(); // + Integer.BYTES
                writer.write_u32::<BE>(data.keys.len() as u32).unwrap(); // numWritten
                writer.write_all(&index_header).unwrap();
                writer.write_all(&index_items).unwrap();
                writer.write_u8(2).unwrap(); // VERSION
                writer.write_u8(vint.size as u8).unwrap();
                writer.write_u32::<BE>(data.len() as u32).unwrap();
                writer.write_u32::<BE>(chunk_factor as u32).unwrap();
                writer.write_u8(conf::vals.compression as u8).unwrap();
                writer.write_u8(1).unwrap(); // VERSION_ONE
                writer.write_u8(0).unwrap(); // REVERSE_LOOKUP_DISALLOWED
                writer.write_u32::<BE>((index_header_c.len() + index_values_c.len()) as u32).unwrap();
                writer.write_all(&index_header_c).unwrap();
                writer.write_all(&index_values_c).unwrap();
            },
        }

        writer.write_u8(1).unwrap(); // VERSION
        writer.write_u8(0).unwrap(); // REVERSE_LOOKUP_DISALLOWED
        writer.write_u32::<BE>(
            (bitmap_header.len() + bitmap_values.len()) as u32,
        ).unwrap();
        writer.write_all(&bitmap_header).unwrap();
        writer.write_all(&bitmap_values).unwrap();
    }

    pub fn sort(&mut self) {
        // TODO: Get the .union method going
        let mut newisf = ISF::new();
        for isf in &self.0 {
            for d in &isf.keys {
                newisf.keys.insert(d.clone());
            }
        }
        newisf.keys.sort();

        for isf in &self.0 {
            for i in &isf.indexes {
                let v = isf.keys.get_index(*i).unwrap();
                let (ci, _) = newisf.keys.get_full(v).unwrap();
                newisf.indexes.push(ci);
            }
        }

        self.0 = vec![newisf];
    }

    pub fn sort_and_permute(&mut self, permutation: &[usize]) {
        self.sort();
        let mut newindexes = vec![0; self.0[0].indexes.len()];
        for (dest_pos, curr_pos) in permutation.iter().enumerate() {
            newindexes[dest_pos] = self.0[0].indexes[*curr_pos];
        }
        self.0[0].indexes = newindexes;
    }
}


#[derive(Debug)]
struct ISF {
    keys: IndexSet<String>,
    indexes: Vec<usize>,
}

impl ISF {
    fn new() -> Self {
        Self{keys: IndexSet::new(), indexes: vec![]}
    }

    fn add_s(&mut self, s: String) {
        let (i, _) = self.keys.insert_full(s);
        self.indexes.push(i);
    }

    fn len(&self) -> usize {
        self.indexes.len()
    }
}
