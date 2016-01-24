use std::io::{self, Write, Seek, BufWriter};
use std::u32;

use index::varint;
use index::writer::{WriteTrigram, get_offset};

use index::byteorder::{BigEndian, WriteBytesExt};
use index::tempfile::TempFile;

pub struct PostDataWriter<W: Write + Seek> {
    pub out: BufWriter<W>,
    pub post_index_file: BufWriter<TempFile>,
    base: u32,
    count: u32,
    offset: u32,
    last: u32,
    t: u32
}

impl<W: Write + Seek> PostDataWriter<W> {
    pub fn new(out: BufWriter<W>) -> io::Result<PostDataWriter<W>> {
        let mut out = out;
        let base = try!(get_offset(&mut out)) as u32;
        Ok(PostDataWriter {
            out: out,
            post_index_file: BufWriter::with_capacity(256 << 10, try!(TempFile::new())),
            base: base,
            count: 0,
            offset: 0,
            last: 0,
            t: 0
        })
    }
    pub fn trigram(&mut self, t: u32) {
        self.offset = get_offset(&mut self.out).unwrap() as u32;
        self.count = 0;
        self.t = t;
        self.last = u32::MAX;
    }
    pub fn file_id(&mut self, id: u32) {
        if self.count == 0 {
            self.out.write_trigram(self.t).unwrap();
        }
        varint::write_uvarint(&mut self.out, id.wrapping_sub(self.last)).unwrap();
        self.last = id;
        self.count += 1;
    }
    pub fn end_trigram(&mut self) {
        if self.count == 0 {
            return;
        }
        varint::write_uvarint(&mut self.out, 0).unwrap();
        self.post_index_file.write_trigram(self.t).unwrap();
        self.post_index_file.write_u32::<BigEndian>(self.count).unwrap();
        self.post_index_file.write_u32::<BigEndian>(self.offset - self.base).unwrap();
    }
}
