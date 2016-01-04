// Copyright 2015 Vernon Jones.
// Original code Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#![allow(dead_code)]
use std::fs::File;
use std::path::Path;
use std::io::{self, Seek, SeekFrom, BufWriter, Write};
use std::ffi::OsString;
use std::ops::Deref;
use std::u32;
use std::mem;

use index::varint;
use index::tempfile::{TempFile, NamedTempFile};
use index::byteorder::{BigEndian, WriteBytesExt};

use index::{MAGIC, TRAILER_MAGIC};

use super::sparseset::SparseSet;
use super::error::{IndexError, IndexErrorKind, IndexResult};
use super::{copy_file, get_offset};
use super::postentry::PostEntry;
use super::postheap::PostHeap;
use super::trigramiter::TrigramIter;
use super::NPOST;

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
const MAX_TEXT_TRIGRAMS: u64 = 30000;
const MAX_INVALID_UTF8_RATION: f64 = 0.1;
const MAX_LINE_LEN: u64 = 2000;


pub struct IndexWriter {
    pub max_trigram_count: u64,
    pub max_utf8_invalid: f64,
    pub max_file_len: u64,
    pub max_line_len: u64,

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
    pub fn new<P: AsRef<Path>>(filename: P) -> IndexWriter {
        let f = File::create(filename).expect("failed to make index!");
        IndexWriter {
            max_trigram_count: MAX_TEXT_TRIGRAMS,
            max_utf8_invalid: MAX_INVALID_UTF8_RATION,
            max_file_len: MAX_FILE_LEN,
            max_line_len: MAX_LINE_LEN,
            paths: Vec::new(),
            name_data: make_temp_buf(),
            name_index: make_temp_buf(),
            trigram: SparseSet::new(),
            number_of_names_written: 0,
            bytes_written: 0,
            post: Vec::with_capacity(NPOST),
            post_files: Vec::new(),
            post_index: make_temp_buf(),
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
        if size > self.max_file_len {
            return Err(IndexError::new(IndexErrorKind::FileTooLong,
                                       format!("file too long, ignoring ({} > {})",
                                               size, self.max_file_len)));
        }
        self.trigram.clear();
        let max_utf8_invalid = ((size as f64) * self.max_utf8_invalid) as u64;
        for each_trigram in TrigramIter::new(f, max_utf8_invalid, self.max_line_len) {
            self.trigram.insert(try!(each_trigram));
        }
        if (self.trigram.len() as u64) > self.max_trigram_count {
            return Err(IndexError::new(IndexErrorKind::TooManyTrigrams,
                                       format!("Too many trigrams ({} > {})",
                                               self.trigram.len(), self.max_trigram_count)));

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
        try!(self.name_data.write(s.as_bytes()));

        self.name_data.write_u8(0).unwrap();

        let id = self.number_of_names_written;
        self.number_of_names_written += 1;
        Ok(id as u32)
    }
    pub fn flush(mut self) -> IndexResult<()> {
        try!(self.add_name(&OsString::new()));
        try!(self.index.write(MAGIC.as_bytes()));

        let mut off = [0; 5];
        off[0] = try!(get_offset(&mut self.index));

        for p in &self.paths {
            try!(self.index.write(p.to_str().unwrap().as_bytes()));
            try!(self.index.write("\0".as_bytes()));
        }
        try!(self.index.write("\0".as_bytes()));
        off[1] = try!(get_offset(&mut self.index));
        try!(self.name_data.flush());
        copy_file(&mut self.index, &mut self.name_data.get_mut());
        off[2] = try!(get_offset(&mut self.index));
        try!(self.merge_post());
        off[3] = try!(get_offset(&mut self.index));
        try!(self.name_index.flush());
        copy_file(&mut self.index, &mut self.name_index.get_mut());
        off[4] = try!(get_offset(&mut self.index));
        try!(self.post_index.flush());
        copy_file(&mut self.index, &mut self.post_index.get_mut());
        for v in off.iter() {
            self.index.write_u32::<BigEndian>(*v as u32).unwrap();
        }
        try!(self.index.write(TRAILER_MAGIC.as_bytes()));
        info!("{} data bytes, {} index bytes",
              self.bytes_written,
              try!(get_offset(&mut self.index)));
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
                varint::write_uvarint(&mut self.index, fdiff).unwrap();
                file_id = e.file_id();
                nfile += 1;
                e = h.next().unwrap_or(PostEntry::new((1<<24)-1, 0));
            }
            varint::write_uvarint(&mut self.index, 0).unwrap();

            self.post_index.write(&mut buf).unwrap();
            self.post_index.write_u32::<BigEndian>(nfile).unwrap();
            self.post_index.write_u32::<BigEndian>(offset as u32).unwrap();

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
}

fn make_temp_buf() -> BufWriter<TempFile> {
    let w = TempFile::new().expect("failed to make tempfile!");
    BufWriter::with_capacity(256 << 10, w)
}
