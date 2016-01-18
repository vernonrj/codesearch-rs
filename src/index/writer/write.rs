// Original code Copyright 2011 The Go Authors.  All rights reserved.
// Original Code Copyright 2013 Manpreet Singh ( junkblocker@yahoo.com ). All rights reserved.
//
// Copyright 2016 Vernon Jones. All rights reserved.
//
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
use super::sort_post::sort_post;
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


/**
 * Creates an index
 *
 * ```no_run
 * # use index::writer::write::IndexWriter;
 * # fn main() {
 * let mut index_writer = IndexWriter::new("index").unwrap();
 * index_writer.add_paths("/path/to/be/indexed").unwrap();
 *
 * for each_file in walk_dir("/path/to/be/indexed") {
 *     index_writer.add_file(each_file).unwrap();
 * }
 * index_writer.flush().unwrap();
 * # }
 * ```
 */
pub struct IndexWriter {

    /// Max number of allowed trigrams in a file
    pub max_trigram_count: u64,
    /// Max percentage of invalid utf-8 sequences allowed
    pub max_utf8_invalid: f64,
    /// Don't index a file if its size in bytes is larger than this
    pub max_file_len: u64,
    /// Stop indexing a file if it has a line longer than this
    pub max_line_len: u64,

    paths: Vec<OsString>,

    name_data: BufWriter<TempFile>,
    name_index: BufWriter<TempFile>,

    trigram: SparseSet,

    /// Tracks the number of names written to disk (used to assign file IDs)
    pub number_of_names_written: usize,
    /// Tracks the total number of bytes written to index
    pub bytes_written: usize,

    post: Vec<PostEntry>,
    post_files: Vec<NamedTempFile>,
    post_index: BufWriter<TempFile>,

    index: BufWriter<File>
}

impl IndexWriter {
    /// Creates a new index file at `filename`
    ///
    /// ```no_run
    /// let index = IndexWriter::new("index").unwrap();
    /// ```
    pub fn new<P: AsRef<Path>>(filename: P) -> io::Result<IndexWriter> {
        let _frame = profiling::profile("IndexWriter::new");
        let f = try!(File::create(filename));
        Ok(IndexWriter {
            max_trigram_count: MAX_TEXT_TRIGRAMS,
            max_utf8_invalid: MAX_INVALID_UTF8_RATION,
            max_file_len: MAX_FILE_LEN,
            max_line_len: MAX_LINE_LEN,
            paths: Vec::new(),
            name_data: try!(make_temp_buf()),
            name_index: try!(make_temp_buf()),
            trigram: SparseSet::new(),
            number_of_names_written: 0,
            bytes_written: 0,
            post: Vec::with_capacity(NPOST),
            post_files: Vec::new(),
            post_index: try!(make_temp_buf()),
            index: BufWriter::with_capacity(256 << 10, f)
        })
    }

    /// Add the specified paths to the index.
    /// Note that this only writes the names of the paths into
    /// the index, it doesn't actually walk those directories.
    /// See `IndexWriter::add_file` for that.
    pub fn add_paths<I: IntoIterator<Item=OsString>>(&mut self, paths: I) {
        self.paths.extend(paths);
    }

    /// Open a file and index it
    ///
    /// ```no_run
    /// let mut index = IndexWriter::open("index");
    /// index.add_file("/path/to/file").unwrap();
    /// index.flush().unwrap();
    /// ```
    pub fn add_file<P: AsRef<Path>>(&mut self, filename: P) -> IndexResult<()> {
        let _frame = profiling::profile("IndexWriter::add_file");
        let f = try!(File::open(filename.as_ref()));
        let metadata = try!(f.metadata());
        self.add(filename, f, metadata.len())
    }

    /// Indexes a file
    ///
    /// `filename` is the name of the opened file referred to by `f`.
    /// `size` is the size of the file referred to by `f`.
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
            let mut trigrams = TrigramReader::new(f, max_utf8_invalid, self.max_line_len);
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

    /// Take trigrams in `trigams` and push them to the post list,
    /// possibly flushing them to file.
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

    /// Add `filename` to the nameData section of the index
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

    /// Finalize the index, collecting all data and writing it out.
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
            try!(self.index.write_u8(0));
        }
        try!(self.index.write_u8(0));
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
    /// Merge the posting lists together
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
            let _fname_diffs = profiling::profile("IndexWriter::merge_post: Write file diffs");
            for each_file in to_diffs(plist.map(|p| p.file_id())) {
                try!(varint::write_uvarint(&mut self.index, each_file));
                written += 1;
            }
            drop(_fname_diffs);

            let _fname_diffs = profiling::profile("IndexWriter::merge_post: Write file diffs");
            try!(self.post_index.write_trigram(plist_trigram));
            try!(self.post_index.write_u32::<BigEndian>(written - 1));
            try!(self.post_index.write_u32::<BigEndian>(offset as u32));
        }
        // NOTE: write last entry like how the go version works
        try!(self.index.write_trigram(0xffffff));           // END trigram
        try!(varint::write_uvarint(&mut self.index, 0));    // NUL byte for END postlist
        try!(self.post_index.write_trigram(0xffffff));      // END trigram
        try!(self.post_index.write_u32::<BigEndian>(0));    // nothing written
        try!(self.post_index.write_u32::<BigEndian>(0));    // offset = 0

        Ok(())
    }

    /// Flush the post data to a temporary file
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

fn make_temp_buf() -> io::Result<BufWriter<TempFile>> {
    let w = try!(TempFile::new());
    Ok(BufWriter::with_capacity(256 << 10, w))
}


