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
use std::mem;

use index::varint;
use index::tempfile::{TempFile, NamedTempFile};
use index::byteorder::{BigEndian, WriteBytesExt};
use index::profiling;

use index::{MAGIC, TRAILER_MAGIC};

use super::sparseset::SparseSet;
use super::error::{IndexError, IndexErrorKind, IndexResult};
use super::{WriteTrigram, copy_file, get_offset};
use super::postinglist::{to_diffs, TakeWhilePeek};
use super::postentry::PostEntry;
use super::postheap::PostHeap;
use super::trigramiter::TrigramReader;
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
    trigram_reader: TrigramReader,

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
        let _frame = profiling::profile("IndexWriter::new");
        let f = File::create(filename).expect("failed to make index!");
        IndexWriter {
            max_trigram_count: MAX_TEXT_TRIGRAMS,
            max_utf8_invalid: MAX_INVALID_UTF8_RATION,
            max_file_len: MAX_FILE_LEN,
            max_line_len: MAX_LINE_LEN,
            paths: Vec::new(),
            name_data: make_temp_buf(),
            name_index: make_temp_buf(),
            trigram_reader: TrigramReader::new(),
            trigram: SparseSet::new(),
            number_of_names_written: 0,
            bytes_written: 0,
            post: Vec::with_capacity(NPOST),
            post_files: Vec::new(),
            post_index: make_temp_buf(),
            index: BufWriter::with_capacity(256 << 10, f)
        }
    }
    pub fn add_paths<I: IntoIterator<Item=OsString>>(&mut self, paths: I) {
        self.paths.extend(paths);
    }
    pub fn add_file<P: AsRef<Path>>(&mut self, filename: P) -> IndexResult<()> {
        let _frame = profiling::profile("IndexWriter::add_file");
        let f = try!(File::open(filename.as_ref()));
        let metadata = try!(f.metadata());
        self.add(filename, f, metadata.len())
    }
    fn add<P: AsRef<Path>>(&mut self, filename: P, f: File, size: u64) -> IndexResult<()> {
        let _frame = profiling::profile("IndexWriter::add");
        if size > self.max_file_len {
            return Err(IndexError::new(IndexErrorKind::FileTooLong,
                                       format!("file too long, ignoring ({} > {})",
                                               size, self.max_file_len)));
        }
        self.trigram.clear();
        let max_utf8_invalid = ((size as f64) * self.max_utf8_invalid) as u64;
        {
            let mut trigrams = self.trigram_reader.open(f, max_utf8_invalid, self.max_line_len);
            let _trigram_insert_frame = profiling::profile("IndexWriter::add: Insert Trigrams");
            while let Some(each_trigram) = trigrams.next() {
                self.trigram.insert(each_trigram);
            }
            if let Some(e) = trigrams.take_error() {
                return e;
            }
        }
        if (self.trigram.len() as u64) > self.max_trigram_count {
            return Err(IndexError::new(IndexErrorKind::TooManyTrigrams,
                                       format!("Too many trigrams ({} > {})",
                                               self.trigram.len(), self.max_trigram_count)));

        }
        debug!("{} {} {:?}", size, self.trigram.len(), filename.as_ref());
        self.bytes_written += size as usize;

        let file_id = try!(self.add_name(filename));
        let v = self.trigram.take_dense();
        self.push_trigrams_to_post(file_id, v)
    }
    fn push_trigrams_to_post(&mut self, file_id: u32, trigrams: Vec<u32>) -> IndexResult<()> {
        let _frame = profiling::profile("IndexWriter::push_trigrams_to_post");
        for each_trigram in trigrams {
            if self.post.len() >= NPOST {
                try!(self.flush_post());
            }
            self.post.push(PostEntry::new(each_trigram, file_id));
        }
        Ok(())
    }
    fn add_name<P: AsRef<Path>>(&mut self, filename: P) -> IndexResult<u32> {
        let _frame = profiling::profile("IndexWriter::add_name");
        let offset = try!(get_offset(&mut self.name_data));
        try!(self.name_index.write_u32::<BigEndian>(offset as u32));

        let s = try!(filename
                     .as_ref()
                     .to_str()
                     .ok_or(IndexError::new(IndexErrorKind::FileNameError,
                                            "UTF-8 Conversion error")));
        try!(self.name_data.write(s.as_bytes()));
        try!(self.name_data.write_u8(0));

        let id = self.number_of_names_written;
        self.number_of_names_written += 1;
        Ok(id as u32)
    }
    pub fn flush(mut self) -> IndexResult<()> {
        let _frame = profiling::profile("IndexWriter::flush");
        try!(self.add_name(""));
        try!(self.index.write(MAGIC.as_bytes()));

        let mut off = [0; 5];
        off[0] = try!(get_offset(&mut self.index));

        for p in &self.paths {
            let path_as_bytes = try!(p.to_str()
                                      .map(str::as_bytes)
                                      .ok_or(IndexError::new(IndexErrorKind::FileNameError,
                                                             "UTF-8 Conversion error")));
            try!(self.index.write(path_as_bytes));
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
            try!(self.index.write_u32::<BigEndian>(*v as u32));
        }
        try!(self.index.write(TRAILER_MAGIC.as_bytes()));
        info!("{} data bytes, {} index bytes",
              self.bytes_written,
              try!(get_offset(&mut self.index)));
        Ok(())
    }
    fn merge_post(&mut self) -> io::Result<()> {
        let _frame = profiling::profile("IndexWriter::merge_post");
        let mut heap = PostHeap::new();
        info!("merge {} files + mem", self.post_files.len());

        for f in &self.post_files {
            try!(heap.add_file(f.deref()));
        }
        sort_post(&mut self.post);
        let mut v = Vec::new();
        mem::swap(&mut v, &mut self.post);
        heap.add_mem(v);

        let mut h = heap.into_iter().peekable();
        let offset0 = try!(get_offset(&mut self.index));

        let _frame_write = profiling::profile("IndexWriter::merge_post: Generate/Write post index");
        while let Some(plist) = TakeWhilePeek::new(&mut h) {
            let _fname_write_to_index = profiling::profile("IndexWriter::merge_post: Write post index");
            let offset = try!(get_offset(&mut self.index)) - offset0;

            // posting list
            let plist_trigram = plist.trigram();
            try!(self.index.write_trigram(plist_trigram));
            let mut written = 0;
            for each_file in to_diffs(plist.map(|p| p.file_id())) {
                try!(varint::write_uvarint(&mut self.index, each_file));
                written += 1;
            }

            try!(self.post_index.write_trigram(plist_trigram));
            try!(self.post_index.write_u32::<BigEndian>(written - 1));
            try!(self.post_index.write_u32::<BigEndian>(offset as u32));
        }
        Ok(())
    }
    fn flush_post(&mut self) -> io::Result<()> {
        let _frame = profiling::profile("IndexWriter::flush_post");
        sort_post(&mut self.post);
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

const K: usize = 12;

fn sort_post(post: &mut Vec<PostEntry>) {
    let _frame = profiling::profile("sort_post");
    let mut sort_tmp = Vec::<PostEntry>::with_capacity(post.len());
    unsafe { sort_tmp.set_len(post.len()) };
    let mut sort_n = [0; 1<<K];
    for p in post.iter() {
        let r = p.trigram() & ((1<<K) - 1);
        sort_n[r as usize] += 1;
    }
    let mut tot = 0;
    for count in sort_n.iter_mut() {
        let val = *count;
        *count = tot;
        tot += val;
    }
    for p in post.iter() {
        let r = (p.trigram() & ((1<<K) - 1)) as usize;
        let o = sort_n[r];
        sort_n[r] += 1;
        sort_tmp[o] = *p;
    }
    mem::swap(post, &mut sort_tmp);

    sort_n = [0; 1<<K];
    for p in post.iter() {
        let r = ((p.value() >> (32+K)) & ((1<<K) - 1)) as usize;
        sort_n[r] += 1;
    }
    tot = 0;
    for count in sort_n.iter_mut() {
        let val = *count;
        *count = tot;
        tot += val;
    }
    for p in post.iter() {
        let r = ((p.value() >> (32+K)) & ((1<<K) - 1)) as usize;
        let o = sort_n[r];
        sort_n[r] += 1;
        sort_tmp[o] = *p;
    }
    mem::swap(post, &mut sort_tmp);
}

#[test]
fn test_sort() {
    let mut v = vec![PostEntry::new(5, 0), PostEntry::new(1, 0), PostEntry::new(10, 0),
                     PostEntry::new(4, 1), PostEntry::new(4, 5), PostEntry::new(5, 10)];
    let mut v_1 = v.clone();
    v_1.sort();
    sort_post(&mut v);
    assert_eq!(v, v_1);
}
