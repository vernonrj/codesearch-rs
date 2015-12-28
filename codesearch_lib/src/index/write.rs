// Copyright 2015 Vernon Jones.
// Original code Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#![allow(dead_code)]
use std::collections::HashSet;
use std::fs::File;
use std::io::{self, Cursor, Seek, SeekFrom, Read, BufRead, BufReader, BufWriter, Write};
use std::io::{Error, ErrorKind};
use std::ffi::OsString;
use std::error;
use std::fmt;
use std::ops::Deref;
use std::u32;

use tempfile::{TempFile, NamedTempFile};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use memmap::{Mmap, Protection};
use varint::VarintWrite;

use index;

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
const NPOST: usize = (64 << 20) / 8;

#[derive(Debug)]
pub struct IndexError {
    kind: IndexErrorKind,
    error: Box<error::Error + Send + Sync>
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexErrorKind {
    IoError,
    FileTooLong,
    TooManyTrigrams,
    BinaryDataPresent
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
            IndexErrorKind::TooManyTrigrams => "too many trigrams in file",
            IndexErrorKind::BinaryDataPresent => "binary data present in file"
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

    name_data: TempFile,
    name_index: TempFile,

    pub number_of_names_written: usize,
    pub bytes_written: usize,

    post: Vec<PostEntry>,
    post_files: Vec<NamedTempFile>,
    post_index: TempFile,

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
            name_data: TempFile::new().expect("failed to make tempfile"),
            name_index: TempFile::new().expect("failed to make tempfile"),
            number_of_names_written: 0,
            bytes_written: 0,
            post: Vec::new(),
            post_files: Vec::new(),
            post_index: TempFile::new().expect("failed to make tempfile"),
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
            trigram.insert(try!(each_trigram));
        }
        println!("indexing file {:?}, num trigrams = {}", filename, trigram.len());
        // TODO: add invalid trigram count checking
        if trigram.len() > MAX_TEXT_TRIGRAMS {
            return Err(IndexError::new(IndexErrorKind::TooManyTrigrams,
                                       "Too many trigrams, ignoring"));
        }
        self.bytes_written += size as usize;

        let file_id = try!(self.add_name(filename));
        // println!("len = {}, cap = {}", self.post.len(), NPOST);
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
    pub fn flush(mut self) -> io::Result<()> {
        self.add_name(OsString::new()).unwrap();
        Self::write_string(&mut self.index, index::MAGIC).unwrap();

        let mut off = [0; 5];
        off[0] = get_offset(&mut self.index).unwrap();

        for p in &self.paths {
            Self::write_string(&mut self.index, &p).unwrap();
            Self::write_string(&mut self.index, "\0").unwrap();
        }
        Self::write_string(&mut self.index, "\0").unwrap();
        off[1] = get_offset(&mut self.index).unwrap();
        copy_file(&mut self.index, &mut self.name_data);
        off[2] = get_offset(&mut self.index).unwrap();
        self.merge_post().unwrap();
        off[3] = get_offset(&mut self.index).unwrap();
        copy_file(&mut self.index, &mut self.name_index);
        off[4] = get_offset(&mut self.index).unwrap();
        copy_file(&mut self.index, &mut self.post_index);
        for v in off.iter() {
            Self::write_u32(&mut self.index, *v as u32).unwrap();
        }
        Self::write_string(&mut self.index, index::TRAILER_MAGIC).unwrap();
        println!("{} data bytes, {} index bytes",
                 self.bytes_written,
                 get_offset(&mut self.index).unwrap());
        Ok(())
    }
    fn merge_post(&mut self) -> io::Result<()> {
        let mut h = PostHeap::new();
        println!("merge {} files + mem", self.post_files.len());

        for f in &self.post_files {
            h.add_file(f.deref()).unwrap();
        }
        self.post.sort();
        // TODO: maybe move out of self.post?
        h.add_mem(self.post.clone());

        let mut npost = 0;
        let mut e = h.next();
        let offset0 = get_offset(&mut self.index).unwrap();

        loop {
            npost += 1;
            let offset = get_offset(&mut self.index).unwrap() - offset0;
            let trigram = e.trigram();
            self.buf[0] = ((trigram >> 16) & 0xff) as u8;
            self.buf[1] = ((trigram >> 8) & 0xff) as u8;
            self.buf[2] = (trigram & 0xff) as u8;

            // posting list
            let mut file_id = u32::MAX;
            let mut nfile = 0;
            self.index.write(&mut self.buf[..3]).unwrap();
            while e.trigram() == trigram && trigram != (1<<24)-1 {
                let mut v = Cursor::new(Vec::<u8>::new());
                let fdiff = {
                    // need to sorta overflow gracefully
                    let fid1 = e.file_id();
                    let fid2 = file_id;
                    if fid1 >= fid2 {
                        fid1 - fid2
                    } else { 
                        fid1 + 1
                    }
                };
                v.write_unsigned_varint_32(fdiff).unwrap();
                self.index.write(v.into_inner().deref()).unwrap();
                file_id = e.file_id();
                nfile += 1;
                e = h.next();
            }
            let mut v = Cursor::new(Vec::<u8>::new());
            v.write_unsigned_varint_32(0).unwrap();
            self.index.write(v.into_inner().deref()).unwrap();

            // FIXME: is buf stil the right value?
            self.post_index.write(&mut self.buf[..3]).unwrap();
            Self::write_u32(&mut self.post_index, nfile).unwrap();
            Self::write_u32(&mut self.post_index, offset as u32).unwrap();

            if trigram == (1<<24)-1 {
                break;
            }
        }
        Ok(())
    }
    fn flush_post(&mut self) -> io::Result<()> {
        self.post.sort();
        let mut f = try!(NamedTempFile::new());
        for each in &self.post {
            try!(f.write_u64::<BigEndian>(each.value()));
        }
        self.post = Vec::new();
        try!(f.seek(SeekFrom::Start(0)));
        self.post_files.push(f);
        Ok(())
    }
    pub fn write_string<W: Write>(writer: &mut W, s: &str) -> io::Result<usize> {
        writer.write(s.as_bytes())
    }
    pub fn write_trigram<W: Write>(writer: &mut W, t: u32) -> io::Result<usize> {
        let mut buf: [u8; 3] = [((t >> 16) & 0xff) as u8,
                                ((t >> 8) & 0xff) as u8,
                                (t & 0xff) as u8];
        writer.write(&mut buf)
    }
    pub fn write_u32<W: Write>(writer: &mut W, u: u32) -> io::Result<usize> {
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
    type Item = IndexResult<u32>;
    fn next(&mut self) -> Option<Self::Item> {
        let c = match self.next_char() {
            Ok(Some(c)) => c,
            Ok(None) => {
                return if self.num_read > 0 && self.num_read < 3 {
                    self.num_read = 0;
                    return Some(Ok(self.current_value));
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
                Some(Err(IndexError::new(IndexErrorKind::BinaryDataPresent,
                                         "Binary data found in file")))
            } else {
                Some(Ok(self.current_value))
            }
        }
    }
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

#[derive(Clone)]
struct PostChunk {
    e: PostEntry,
    m: Vec<PostEntry>
}

struct PostHeap {
    ch: Vec<PostChunk>
}

impl PostHeap {
    pub fn new() -> PostHeap {
        PostHeap {
            ch: Vec::new()
        }
    }
    pub fn len(&self) -> usize { self.ch.len() }
    pub fn is_empty(&self) -> bool { self.ch.is_empty() }
    pub fn next(&mut self) -> PostEntry {
        if self.ch.is_empty() {
            return PostEntry::new((1 << 24) - 1, 0);
        }
        let e = self.ch[0].e;
        if self.ch[0].m.is_empty() {
            self.pop();
        } else {
            self.ch[0].e = self.ch[0].m[0];
            self.ch[0].m.remove(0);
            self.sift_down(0);
        }
        return e;
    }
    pub fn add_file(&mut self, f: &File) -> io::Result<()> {
        let m = try!(Mmap::open(f, Protection::Read));
        let mut bytes = Cursor::new(unsafe { m.as_slice() });
        let mut v = Vec::new();
        while let Ok(p) = bytes.read_u64::<BigEndian>() {
            v.push(PostEntry(p));
        }
        self.add_mem(v);
        Ok(())
    }
    pub fn add_mem(&mut self, v: Vec<PostEntry>) {
        self.add(PostChunk { e: PostEntry(0), m: v });
    }
    pub fn add(&mut self, ch: PostChunk) {
        let mut ch = ch;
        if ch.m.len() > 0 {
            ch.e = ch.m[0];
            ch.m.remove(0);
            self.push(ch);
        }
    }
    pub fn push(&mut self, ch: PostChunk) {
        let n = self.ch.len();
        self.ch.push(ch);
        if self.ch.len() >= 2 {
            self.sift_up(n);
        }
    }
    pub fn pop(&mut self) -> Option<PostChunk> {
        if self.is_empty() {
            return None;
        }
        let ch = self.ch[0].clone();
        let n = self.ch.len() - 1;
        self.ch.swap(0, n);
        self.ch.pop();
        if n > 1 {
            self.sift_down(0);
        }
        Some(ch)
    }
    pub fn sift_up(&mut self, mut j: usize) {
        loop {
            if j == 0 {
                break;
            }
            let i = (j - 1) / 2;
            if i == j || self.ch[i].e < self.ch[j].e {
                break;
            }
            self.ch.swap(i, j);
            j = i;
        }
    }
    pub fn sift_down(&mut self, mut i: usize) {
        loop {
            let j1 = 2*i + 1;
            if j1 >= self.ch.len() {
                break;
            }
            let mut j = j1;
            let j2 = j1 + 1;
            if j2 < self.ch.len() && self.ch[j1].e >= self.ch[j2].e {
                j = j2;
            }
            if self.ch[i].e < self.ch[j].e {
                break;
            }
            self.ch.swap(i, j);
            i = j;
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