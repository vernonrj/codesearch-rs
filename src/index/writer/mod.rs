// Copyright 2016 Vernon Jones. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

use std::io::{self, BufReader, BufWriter, SeekFrom, BufRead, Read, Seek, Write};

pub use self::write::IndexWriter;
pub use self::error::{IndexResult, IndexError, IndexErrorKind};


mod write;
mod error;
mod sparseset;

mod postinglist;
mod postentry;
mod postheap;
mod trigramiter;
mod sort_post;

const NPOST: usize = (64 << 20) / 8;

pub fn get_offset<S: Seek>(seekable: &mut S) -> io::Result<u64> {
    seekable.seek(SeekFrom::Current(0))
}

pub fn copy_file<R: Read + Seek, W: Write>(dest: &mut BufWriter<W>, src: &mut R) {
    src.seek(SeekFrom::Start(0)).unwrap();
    let mut buf_src = BufReader::new(src); 
    loop {
        let length = if let Ok(b) = buf_src.fill_buf() {
            if b.len() == 0 {
                break;
            }
            dest.write_all(b).unwrap();
            b.len()
        } else {
            break;
        };
        buf_src.consume(length);
    }
}


pub trait WriteTrigram: Write {
    fn write_trigram(&mut self, t: u32) -> io::Result<()> {
        let mut buf: [u8; 3] = [((t >> 16) & 0xff) as u8,
                                ((t >> 8) & 0xff) as u8,
                                (t & 0xff) as u8];
        self.write_all(&mut buf)
    }
}

impl<W: Write + ?Sized> WriteTrigram for W {}
