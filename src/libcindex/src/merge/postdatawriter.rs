// Copyright 2016 Vernon Jones.
// Original code Copyright 2013 Manpreet Singh ( junkblocker@yahoo.com ). All rights reserved.
// Original code Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

use std::io::{self, Write, Seek, BufWriter};
use std::u32;

use libvarint;
use writer::{WriteTrigram, get_offset};

use byteorder::{BigEndian, WriteBytesExt};
use tempfile::TempFile;

pub struct PostDataWriter<'a, W: 'a + Write + Seek> {
    out: &'a mut BufWriter<W>,
    post_index_file: BufWriter<TempFile>,
    base: u32,
    count: u32,
    offset: u32,
    last: u32,
    t: u32,
}

impl<'a, W: Write + Seek> PostDataWriter<'a, W> {
    pub fn new(out: &'a mut BufWriter<W>) -> io::Result<Self> {
        let base = try!(get_offset(out)) as u32;
        Ok(PostDataWriter {
            out: out,
            post_index_file: BufWriter::with_capacity(256 << 10, try!(TempFile::new())),
            base: base,
            count: 0,
            offset: 0,
            last: 0,
            t: 0,
        })
    }
    pub fn trigram(&mut self, t: u32) {
        self.offset = get_offset(self.out).unwrap() as u32;
        self.count = 0;
        self.t = t;
        self.last = u32::MAX;
    }
    pub fn file_id(&mut self, id: u32) {
        if self.count == 0 {
            self.out.write_trigram(self.t).unwrap();
        }
        libvarint::write_uvarint(self.out, id.wrapping_sub(self.last)).unwrap();
        self.last = id;
        self.count += 1;
    }
    pub fn end_trigram(&mut self) {
        if self.count == 0 {
            return;
        }
        libvarint::write_uvarint(self.out, 0).unwrap();
        self.post_index_file.write_trigram(self.t).unwrap();
        self.post_index_file.write_u32::<BigEndian>(self.count).unwrap();
        self.post_index_file.write_u32::<BigEndian>(self.offset - self.base).unwrap();
    }
    pub fn into_inner(self) -> BufWriter<TempFile> {
        self.post_index_file
    }
}
