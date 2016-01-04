// Copyright 2015 Vernon Jones.
// Original code Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#![allow(dead_code)]
use std::vec;
use std::fs::File;
use std::path::Path;
use std::io::{self, Cursor, Seek, SeekFrom, Read, BufRead, BufReader, BufWriter, Write};
use std::io::Error;
use std::ffi::OsString;
use std::error;
use std::fmt;
use std::ops::Deref;
use std::{u32, u64};
use std::mem;
use std::iter::Peekable;

use index::tempfile::{TempFile, NamedTempFile};
use index::byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use index::memmap::{Mmap, Protection};

use index;
use index::sparseset::SparseSet;

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
const MAX_LINE_LEN: u64 = 2000;
const MAX_TEXT_TRIGRAMS: usize = 30000;
const MAX_INVALID_UTF8_RATION: f64 = 0.1;
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
    LineTooLong,
    TooManyTrigrams,
    BinaryDataPresent,
    HighInvalidUtf8Ratio
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
            IndexErrorKind::LineTooLong => "line too long",
            IndexErrorKind::TooManyTrigrams => "too many trigrams in file",
            IndexErrorKind::BinaryDataPresent => "binary file",
            IndexErrorKind::HighInvalidUtf8Ratio => "Too many invalid utf-8 sequences"
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
    paths: Vec<OsString>,

    name_data: BufWriter<TempFile>,
    name_index: BufWriter<TempFile>,

    trigram: SparseSet,

    pub number_of_names_written: usize,
    pub bytes_written: usize,

    post: Vec<PostEntry>,
    post_files: Vec<NamedTempFile>,
    post_index: BufWriter<TempFile>,

    index: BufWriter<File>
}

impl IndexWriter {
    fn make_temp_buf() -> BufWriter<TempFile> {
        let w = TempFile::new().expect("failed to make tempfile!");
        BufWriter::with_capacity(256 << 10, w)
    }
    pub fn new<P: AsRef<Path>>(filename: P) -> IndexWriter {
        let f = File::create(filename).expect("failed to make index!");
        IndexWriter {
            paths: Vec::new(),
            name_data: Self::make_temp_buf(),
            name_index: Self::make_temp_buf(),
            trigram: SparseSet::new(),
            number_of_names_written: 0,
            bytes_written: 0,
            post: Vec::with_capacity(NPOST),
            post_files: Vec::new(),
            post_index: Self::make_temp_buf(),
            index: BufWriter::with_capacity(256 << 10, f)
        }
    }
    pub fn add_paths(&mut self, paths: Vec<OsString>) {
        self.paths.extend(paths);
    }
    pub fn add_file(&mut self, filename: &OsString) -> IndexResult<()> {
        let f = try!(File::open(filename));
        let metadata = try!(f.metadata());
        self.add(filename, f, metadata.len())
    }
    fn add(&mut self, filename: &OsString, f: File, size: u64) -> IndexResult<()> {
        if size > MAX_FILE_LEN {
            // writeln!(&mut io::stderr(), "{}: file too long, ignoring", filename);
            return Err(IndexError::new(IndexErrorKind::FileTooLong,
                                       format!("too long, ignoring ({} > {})",
                                               size, MAX_FILE_LEN)));
        }
        self.trigram.clear();
        let max_utf8_invalid = ((size as f64) * MAX_INVALID_UTF8_RATION) as u64;
        for each_trigram in TrigramIter::new(f, max_utf8_invalid) {
            self.trigram.insert(try!(each_trigram));
        }
        // TODO: add invalid trigram count checking
        if self.trigram.len() > MAX_TEXT_TRIGRAMS {
            return Err(IndexError::new(IndexErrorKind::TooManyTrigrams,
                                       format!("Too many trigrams ({} > {})",
                                               self.trigram.len(), MAX_TEXT_TRIGRAMS)));

        }
        debug!("{} {} {:?}", size, self.trigram.len(), filename);
        self.bytes_written += size as usize;

        let file_id = try!(self.add_name(filename));
        let mut v = Vec::<u32>::new();
        mem::swap(&mut v, &mut self.trigram.dense_mut());
        for each_trigram in v {
            if self.post.len() >= NPOST {
                try!(self.flush_post());
            }
            self.post.push(PostEntry::new(each_trigram, file_id));
        }
        Ok(())
    }
    fn add_name(&mut self, filename: &OsString) -> IndexResult<u32> {
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
        self.add_name(&OsString::new()).unwrap();
        Self::write_string(&mut self.index, index::MAGIC).unwrap();

        let mut off = [0; 5];
        off[0] = get_offset(&mut self.index).unwrap();

        for p in &self.paths {
            Self::write_string(&mut self.index, p.to_str().unwrap()).unwrap();
            Self::write_string(&mut self.index, "\0").unwrap();
        }
        Self::write_string(&mut self.index, "\0").unwrap();
        off[1] = get_offset(&mut self.index).unwrap();
        self.name_data.flush().unwrap();
        copy_file(&mut self.index, &mut self.name_data.get_mut());
        off[2] = get_offset(&mut self.index).unwrap();
        self.merge_post().unwrap();
        off[3] = get_offset(&mut self.index).unwrap();
        self.name_index.flush().unwrap();
        copy_file(&mut self.index, &mut self.name_index.get_mut());
        off[4] = get_offset(&mut self.index).unwrap();
        self.post_index.flush().unwrap();
        copy_file(&mut self.index, &mut self.post_index.get_mut());
        for v in off.iter() {
            Self::write_u32(&mut self.index, *v as u32).unwrap();
        }
        Self::write_string(&mut self.index, index::TRAILER_MAGIC).unwrap();
        info!("{} data bytes, {} index bytes",
              self.bytes_written,
              get_offset(&mut self.index).unwrap());
        Ok(())
    }
    fn merge_post(&mut self) -> io::Result<()> {
        let mut heap = PostHeap::new();
        info!("merge {} files + mem", self.post_files.len());

        for f in &self.post_files {
            heap.add_file(f.deref()).unwrap();
        }
        self.post.sort();
        let mut v = Vec::new();
        mem::swap(&mut v, &mut self.post);
        heap.add_mem(v);

        let mut h = heap.into_iter();
        let mut e = h.next().unwrap_or(PostEntry::new((1<<24)-1, 0));
        let offset0 = get_offset(&mut self.index).unwrap();

        loop {
            let offset = get_offset(&mut self.index).unwrap() - offset0;
            let trigram = e.trigram();
            let mut buf: [u8; 3] = [
                ((trigram >> 16) & 0xff) as u8,
                ((trigram >> 8) & 0xff) as u8,
                (trigram & 0xff) as u8];

            // posting list
            let mut file_id = u32::MAX;
            let mut nfile: u32 = 0;
            self.index.write(&mut buf).unwrap();
            while e.trigram() == trigram && (trigram != (1<<24)-1) {
                let fdiff = e.file_id().wrapping_sub(file_id);
                IndexWriter::write_uvarint(&mut self.index, fdiff).unwrap();
                file_id = e.file_id();
                nfile += 1;
                e = h.next().unwrap_or(PostEntry::new((1<<24)-1, 0));
            }
            IndexWriter::write_uvarint(&mut self.index, 0).unwrap();

            self.post_index.write(&mut buf).unwrap();
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
        let mut f = BufWriter::with_capacity(NPOST, try!(NamedTempFile::new()));
        debug!("flush {} entries to tempfile", self.post.len());
        for each in &self.post {
            try!(f.write_u64::<BigEndian>(each.value()));
        }
        self.post.clear();
        try!(f.seek(SeekFrom::Start(0)));
        self.post_files.push(try!(f.into_inner()));
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
    pub fn write_uvarint<W: Write>(writer: &mut W, x: u32) -> io::Result<usize> {
        if x < (1<<7) {
            writer.write(&mut [x as u8])
        } else if x < (1<<14) {
            writer.write(&mut [((x | 0x80) & 0xff) as u8,
                               ((x >> 7) & 0xff) as u8])
        } else if x < (1<<21) {
            writer.write(&mut [((x | 0x80) & 0xff) as u8,
                               ((x >> 7) & 0xff) as u8,
                               ((x >> 14) & 0xff) as u8])
        } else if x < (1<<28) {
            writer.write(&mut [((x | 0x80) & 0xff) as u8,
                               ((x >> 7) & 0xff) as u8,
                               ((x >> 14) & 0xff) as u8,
                               ((x >> 21) & 0xff) as u8])
        } else {
            writer.write(&mut [((x | 0x80) & 0xff) as u8,
                               ((x >> 7) & 0xff) as u8,
                               ((x >> 14) & 0xff) as u8,
                               ((x >> 21) & 0xff) as u8,
                               ((x >> 21) & 0xff) as u8])
        }
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

struct TrigramIter<R: Read> {
    reader: io::Bytes<BufReader<R>>,
    current_value: u32,
    num_read: usize,
    inv_cnt: u64,
    max_invalid: u64,
    line_len: u64
}

impl<R: Read> TrigramIter<R> {
    fn new(r: R, max_invalid: u64) -> TrigramIter<R> {
        TrigramIter {
            reader: BufReader::with_capacity(16384, r).bytes(),
            current_value: 0,
            num_read: 0,
            inv_cnt: 0,
            max_invalid: max_invalid,
            line_len: 0
        }
    }
    fn next_char(&mut self) -> io::Result<Option<u8>> {
        match self.reader.next() {
            Some(Err(e)) => Err(e),
            Some(Ok(c)) => {
                self.num_read += 1;
                Ok(Some(c))
            },
            None => Ok(None)
        }
    }
}

impl<R: Read> Iterator for TrigramIter<R> {
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
                Some(Err(IndexError::new(IndexErrorKind::BinaryDataPresent,
                                         format!("Binary File. Bytes {:02x}{:02x} at offset {}",
                                                 b1, b2, self.num_read))))
            } else if !valid_utf8(b1 as u8, b2 as u8) {
                // invalid utf8 data
                self.inv_cnt += 1;
                if self.inv_cnt > self.max_invalid {
                    Some(Err(IndexError::new(IndexErrorKind::HighInvalidUtf8Ratio,
                                             format!("High invalid UTF-8 ratio. total {} invalid: {} ratio: {}",
                                                     self.num_read, self.inv_cnt,
                                                     (self.inv_cnt as f64) / (self.num_read as f64)
                                                     ))))
                } else {
                    return self.next();
                }
            } else if self.line_len > MAX_LINE_LEN {
                Some(Err(IndexError::new(IndexErrorKind::LineTooLong,
                                         format!("Very long lines ({})", self.line_len))))
            } else {
                if c == ('\n' as u8) { 
                    self.line_len = 0;
                } else {
                    self.line_len += 1;
                }
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

struct PostHeap {
    ch: Vec<Peekable<vec::IntoIter<PostEntry>>>
}

impl PostHeap {
    pub fn new() -> PostHeap {
        PostHeap {
            ch: Vec::new()
        }
    }
    pub fn len(&self) -> usize { self.ch.len() }
    pub fn is_empty(&self) -> bool { self.ch.is_empty() }
    pub fn add_file(&mut self, f: &File) -> io::Result<()> {
        let m = try!(Mmap::open(f, Protection::Read));
        let mut bytes = Cursor::new(unsafe { m.as_slice() });
        let mut ch = Vec::with_capacity(NPOST);
        while let Ok(p) = bytes.read_u64::<BigEndian>() {
            ch.push(PostEntry(p));
        }
        self.ch.push(ch.into_iter().peekable());
        Ok(())
    }
    pub fn add_mem(&mut self, v: Vec<PostEntry>) {
        self.ch.push(v.into_iter().peekable());
    }
}

impl IntoIterator for PostHeap {
    type Item = PostEntry;
    type IntoIter = PostHeapIntoIter;
    fn into_iter(self) -> Self::IntoIter {
        PostHeapIntoIter {
            inner: self
        }
    }
}

struct PostHeapIntoIter {
    inner: PostHeap
}

impl PostHeapIntoIter {
    pub fn new(inner: PostHeap) -> Self {
        PostHeapIntoIter {
            inner: inner
        }
    }
}

impl Iterator for PostHeapIntoIter {
    type Item = PostEntry;
    fn next(&mut self) -> Option<Self::Item> {
        let min_idx = if self.inner.ch.is_empty() {
            return None;
        } else if self.inner.ch.len() == 1 {
            0
        } else {
            let mut min_idx = 0;
            let mut min_val = PostEntry(u64::MAX);
            for (each_idx, each_vec) in self.inner.ch.iter_mut().enumerate() {
                let each_val = if let Some(each_val) = each_vec.peek() {
                    each_val
                } else {
                    continue;
                };
                if *each_val < min_val {
                    min_val = *each_val;
                    min_idx = each_idx;
                }
            }
            min_idx
        };
        let min_val = self.inner.ch[min_idx].next().unwrap();
        if self.inner.ch[min_idx].peek().is_none() {
            self.inner.ch.remove(min_idx).last();
        }
        Some(min_val)
    }
}


fn valid_utf8(c1: u8, c2: u8) -> bool {
    if c1 < 0x80 {
        // 1-byte, must be followed by 1-byte or first of multi-byte
        (c2 < 0x80) || (0xc0 <= c2) && (c2 < 0xf8)
    } else if c1 < 0xc0 {
        // continuation byte, can be followed by nearly anything
        (c2 < 0xf8)
    } else if c1 < 0xf8 {
        // first of multi-byte, must be followed by continuation byte
        (0x80 <= c2) && (c2 < 0xc0)
    } else {
        false
    }
}

#[test]
fn test_trigram_iter_once() {
    let c = TrigramIter::new("hello".as_bytes(), 0).next().unwrap();
    let hel =   ('h' as u32) << 16
              | ('e' as u32) << 8
              | ('l' as u32);
    assert!(c.unwrap() == hel);
}

#[test]
pub fn test_trigram_iter() {
    let trigrams: Vec<u32> = TrigramIter::new("hello".as_bytes(), 0)
        .map(Result::unwrap)
        .collect();
    let hel =   ('h' as u32) << 16
              | ('e' as u32) << 8
              | ('l' as u32);
    let ell =   ('e' as u32) << 16
              | ('l' as u32) << 8
              | ('l' as u32);
    let llo =   ('l' as u32) << 16
              | ('l' as u32) << 8
              | ('o' as u32);
    assert!(trigrams == vec![hel,ell,llo]);
}