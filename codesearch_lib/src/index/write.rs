// Copyright 2015 Vernon Jones.
// Original code Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#![allow(dead_code)]
use std::collections::HashSet;
use std::fs::File;
use std::io::{self, Seek, SeekFrom, Read, BufWriter, Write};
use std::io::{Error, ErrorKind};
use std::ffi::OsString;
use std::error;
use std::fmt;

use tempfile::TempFile;
use byteorder::{BigEndian, WriteBytesExt};
use varint::VarintRead;

// Index writing.  See read.rs for details of on-disk format.
//
// It would suffice to make a single large list of (trigram, file#) pairs
// while processing the files one at a time, sort that list by trigram,
// and then create the posting lists from subsequences of the list.
// However, we do not assume that the entire index fits in memory.
// Instead, we sort and flush the list to a new temporary file each time
// it reaches its maximum in-memory size, and then at the end we
// create the final posting lists by merging the temporary files as we
// read them back in.
//
// It would also be useful to be able to create an index for a subset
// of the files and then merge that index into an existing one.  This would
// allow incremental updating of an existing index when a directory changes.
// But we have not implemented that.

const MAX_FILE_LEN: u64 = 1 << 30;
const MAX_LINE_LEN: usize = 2000;
const MAX_TEXT_TRIGRAMS: usize = 20000;
const MAX_INVALID_UTF8_RATION: f64 = 0.0;
const NPOST: usize = 64 << 20 / 8;

#[derive(Debug)]
pub struct IndexError {
    kind: IndexErrorKind,
    error: Box<error::Error + Send + Sync>
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexErrorKind {
    IoError,
    FileTooLong,
    TooManyTrigrams
}


impl IndexError {
    pub fn new<E>(kind: IndexErrorKind, error: E) -> IndexError
        where E: Into<Box<error::Error + Send + Sync>>
    {
        IndexError {
            kind: kind,
            error: error.into()
        }
    }
    pub fn kind(&self) -> IndexErrorKind {
        self.kind.clone()
    }
}

impl From<io::Error> for IndexError {
    fn from(e: io::Error) -> Self {
        IndexError {
            kind: IndexErrorKind::IoError,
            error: Box::new(e)
        }
    }
}

impl error::Error for IndexError {
    fn description(&self) -> &str {
        match self.kind {
            IndexErrorKind::IoError => self.error.description(),
            IndexErrorKind::FileTooLong => "file too long",
            IndexErrorKind::TooManyTrigrams => "too many trigrams in file"
        }
    }
}

impl fmt::Display for IndexError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        self.error.fmt(fmt)
    }
}


pub type IndexResult<T> = Result<T, IndexError>;

pub struct IndexWriter {
    buf: [u8; 8],
    paths: Vec<String>,

    name_data: BufWriter<TempFile>,
    name_index: BufWriter<TempFile>,

    number_of_names_written: usize,
    bytes_written: usize,

    post: Vec<PostEntry>,
    post_files: Vec<TempFile>,
    post_index: BufWriter<TempFile>,

    input_buffer: [u8; 16384],

    index: BufWriter<File>
}

impl IndexWriter {
    fn make_temp_buf() -> BufWriter<TempFile> {
        let w = TempFile::new().expect("failed to make tempfile!");
        BufWriter::new(w)
    }
    // TODO: use Path
    pub fn new(filename: String) -> IndexWriter {
        let f = File::create(filename).expect("failed to make index!");
        IndexWriter {
            buf: [0; 8],
            paths: Vec::new(),
            name_data: Self::make_temp_buf(),
            name_index: Self::make_temp_buf(),
            number_of_names_written: 0,
            bytes_written: 0,
            post: Vec::new(),
            post_files: Vec::new(),
            post_index: Self::make_temp_buf(),
            input_buffer: [0; 16384],
            index: BufWriter::new(f)
        }
    }
    pub fn add_paths(&mut self, paths: Vec<String>) {
        self.paths.extend(paths);
    }
    pub fn add_file(&mut self, filename: OsString) -> IndexResult<()> {
        let f = try!(File::open(filename.clone()));
        let metadata = try!(f.metadata());
        self.add(filename, f, metadata.len())
    }
    fn add(&mut self, filename: OsString, f: File, size: u64) -> IndexResult<()> {
        if size > MAX_FILE_LEN {
            // writeln!(&mut io::stderr(), "{}: file too long, ignoring", filename);
            return Err(IndexError::new(IndexErrorKind::FileTooLong,
                                       "file too long, ignoring"));
        }
        let mut trigram = HashSet::<u32>::new();
        for each_trigram in TrigramIter::from_file(f) {
            trigram.insert(each_trigram);
        }
        // TODO: add invalid trigram count checking
        if trigram.len() > MAX_TEXT_TRIGRAMS {
            return Err(IndexError::new(IndexErrorKind::TooManyTrigrams,
                                       "Too many trigrams, ignoring"));
        }
        self.bytes_written += size as usize;

        let file_id = try!(self.add_name(filename));
        for each_trigram in trigram {
            if self.post.len() >= NPOST {
                try!(self.flush_post());
            }
            self.post.push(PostEntry::new(each_trigram, file_id));
        }
        Ok(())
    }
    fn add_name(&mut self, filename: OsString) -> IndexResult<u32> {
        let offset = try!(get_offset(&mut self.name_data));
        self.name_index.write_u32::<BigEndian>(offset as u32).unwrap();
        let s = filename.to_str().expect("filename has invalid UTF-8 chars");
        try!(Self::write_string(&mut self.name_data, s));

        self.name_data.write_u8(0).unwrap();

        let id = self.number_of_names_written;
        self.number_of_names_written += 1;
        Ok(id as u32)
    }
    fn flush_post(&mut self) -> io::Result<()> {
        self.post.sort();
        let mut f = try!(TempFile::new());
        for each in &self.post {
            try!(f.write_u64::<BigEndian>(each.value()));
        }
        self.post = Vec::new();
        try!(f.seek(SeekFrom::Start(0)));
        self.post_files.push(f);
        Ok(())
    }
    pub fn write_string<W: Write>(writer: &mut BufWriter<W>, s: &str) -> io::Result<usize> {
        writer.write(s.as_bytes())
    }
    pub fn write_trigram<W: Write>(writer: &mut BufWriter<W>, t: u32) -> io::Result<usize> {
        let mut buf: [u8; 3] = [((t >> 16) & 0xff) as u8,
                                ((t >> 8) & 0xff) as u8,
                                (t & 0xff) as u8];
        writer.write(&mut buf)
    }
    pub fn write_u32<W: Write>(writer: &mut BufWriter<W>, u: u32) -> io::Result<usize> {
        let mut buf: [u8; 4] = [((u >> 24) & 0xff) as u8,
                                ((u >> 16) & 0xff) as u8,
                                ((u >> 8) & 0xff) as u8,
                                (u & 0xff) as u8];
        writer.write(&mut buf)
    }
}

pub fn get_offset<S: Seek>(seekable: &mut S) -> io::Result<u64> {
    seekable.seek(SeekFrom::Current(0))
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
struct PostEntry(u64);

impl PostEntry {
    pub fn new(trigram: u32, file_id: u32) -> Self {
        PostEntry((trigram as u64) << 32 | (file_id as u64))
    }
    pub fn trigram(&self) -> u32 {
        let &PostEntry(ref u) = self;
        return (u >> 32) as u32;
    }
    pub fn file_id(&self) -> u32 {
        let &PostEntry(ref u) = self;
        return (u & 0xffffffff) as u32;
    }
    pub fn value(&self) -> u64 {
        let &PostEntry(v) = self;
        v
    }
}

struct RingBuffer {
    buf: [u8; 8],
    read_index: usize,
    write_index: usize,
    num_bytes: usize
}

impl RingBuffer {
    pub fn new() -> RingBuffer {
        RingBuffer {
            buf: [0; 8],
            read_index: 0,
            write_index: 0,
            num_bytes: 0
        }
    }
    pub fn with_buf_mut<F>(&mut self, f: F) -> io::Result<usize>
        where F: FnOnce(&mut [u8]) -> io::Result<usize>
    {
        if self.len() > 0 {
            panic!("not all data has been consumed from RingBuffer");
        }
        let new_size = try!(f(&mut self.buf));
        self.read_index = 0;
        self.write_index = new_size;
        self.num_bytes = new_size;
        Ok(new_size)
    }
    pub fn len(&self) -> usize {
        self.num_bytes
    }
    pub fn capacity() -> usize {
        8
    }
    pub fn is_full(&self) -> bool { self.len() >= Self::capacity() }
    pub fn is_empty(&self) -> bool { self.len() == 0 }
    pub fn push(&mut self, value: u8) -> Option<()> {
        if self.is_full() {
            None
        } else {
            self.buf[self.write_index] = value;
            self.write_index += 1;
            self.num_bytes += 1;
            if self.write_index >= Self::capacity() {
                self.write_index -= Self::capacity();
            }
            Some(())
        }
    }
    pub fn read(&mut self) -> Option<u8> {
        if self.is_empty() {
            None
        } else {
            let value = self.buf[self.read_index];
            self.read_index += 1;
            self.num_bytes -= 1;
            if self.read_index >= Self::capacity() {
                self.read_index -= Self::capacity();
            }
            Some(value)
        }
    }
}

struct TrigramIter {
    reader: Box<Read>,
    buffer: RingBuffer,
    current_value: u32,
    num_read: usize
}

impl TrigramIter {
    fn new(r: Box<Read>) -> TrigramIter {
        TrigramIter {
            reader: r,
            buffer: RingBuffer::new(),
            current_value: 0,
            num_read: 0
        }
    }
    pub fn from_file(f: File) -> TrigramIter {
        TrigramIter {
            reader: Box::new(f),
            buffer: RingBuffer::new(),
            current_value: 0,
            num_read: 0
        }
    }
    fn read_into_buf(&mut self) -> io::Result<usize> {
        let reader = &mut self.reader;
        self.buffer.with_buf_mut(|mut b| reader.read(&mut b))
    }
    fn next_char(&mut self) -> io::Result<Option<u8>> {
        if self.buffer.is_empty() {
            loop {
                match self.read_into_buf() {
                    Ok(0) => return Ok(None),      // no more bytes to read
                    Ok(_) => break,
                    Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                    Err(e) => {
                        writeln!(&mut io::stderr(), "failed to read from file. {}", e).unwrap();
                        return Err(e)
                    }
                }
            }
        }
        self.num_read += 1;
        Ok(self.buffer.read())
    }
}

impl Iterator for TrigramIter {
    type Item = u32;
    fn next(&mut self) -> Option<Self::Item> {
        let c = match self.next_char() {
            Ok(Some(c)) => c,
            Ok(None) => {
                return if self.num_read > 0 && self.num_read < 3 {
                    self.num_read = 0;
                    return Some(self.current_value);
                } else {
                    return None;
                };
            }
            Err(_) => return None     // done with error
        };
        self.current_value = ((1 << 24) - 1) & ((self.current_value << 8) | (c as u32));
        if self.num_read < 3 {
            return self.next();
        } else {
            let b1 = (self.current_value >> 8) & 0xff;
            let b2 = self.current_value & 0xff;
            if b1 == 0x00 || b2 == 0x00 {
                // Binary file. Skip
                // TODO: log when a binary file causes a skip
                None
            } else {
                Some(self.current_value)
            }
        }
    }
}

#[test]
fn test_ringbuffer_init_zero() {
    let r = RingBuffer::new();
    assert!(r.len() == 0);
    assert!(r.is_empty());
}

#[test]
fn test_ringbuffer_push() {
    let mut r = RingBuffer::new();
    assert!(r.push(1).is_some());
    assert!(r.len() == 1);
    assert!(r.push(5).is_some());
    assert!(r.len() == 2);
    let mut counter = 0;
    while r.len() < RingBuffer::capacity() && counter < 10 {
        assert!(r.push(10).is_some());
        counter += 1;
    }
    if counter >= 10 {
        panic!("push isn't incrementing correctly (len == {})!", r.len());
    }
    assert!(r.is_full());
    assert!(r.len() == RingBuffer::capacity());
    assert!(r.push(5).is_none());
}

#[test]
fn test_ringbuffer_pop() {
    let mut r = RingBuffer::new();
    assert!(r.push(1).is_some());
    assert!(r.read() == Some(1));
    assert!(r.len() == 0);
    assert!(r.is_empty());
    assert!(r.read() == None);
}

#[test]
fn test_ringbuffer_read() {
    let mut r = RingBuffer::new();
    let rslt = r.with_buf_mut(|mut b| {
        b[0] = 1;
        b[1] = 2;
        b[2] = 3;
        Ok(3)
    });
    assert!(rslt.is_ok());
    assert!(rslt.unwrap() == 3);
    assert!(r.len() == 3);
    assert!(r.read() == Some(1));
    assert!(r.read() == Some(2));
    assert!(r.read() == Some(3));
    assert!(r.read() == None);
    assert!(r.is_empty());
}

#[test]
fn test_trigram_iter_once() {
    let c = TrigramIter::new(Box::new("hello".as_bytes())).next().unwrap();
    let hel =   ('h' as u32) << 16
              | ('e' as u32) << 8
              | ('l' as u32);
    assert!(c == hel);
}

#[test]
pub fn test_trigram_iter() {
    let trigrams: Vec<u32> = TrigramIter::new(Box::new("hello".as_bytes())).collect();
    let hel =   ('h' as u32) << 16
              | ('e' as u32) << 8
              | ('l' as u32);
    let ell =   ('e' as u32) << 16
              | ('l' as u32) << 8
              | ('l' as u32);
    let llo =   ('l' as u32) << 16
              | ('l' as u32) << 8
              | ('o' as u32);
    println!("{:?} == {:?}", trigrams, vec![hel, ell, llo]);
    assert!(trigrams == vec![hel,ell,llo]);
}
