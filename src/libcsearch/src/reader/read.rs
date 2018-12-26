// Copyright 2015 Vernon Jones.
// Original code Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Index format.
//
// An index stored on disk has the format:
//
// 	"csearch index 1\n"
// 	list of paths
// 	list of names
// 	list of posting lists
// 	name index
// 	posting list index
// 	trailer
//
// The list of paths is a sorted sequence of NUL-terminated file or directory names.
// The index covers the file trees rooted at those paths.
// The list ends with an empty name ("\x00").
//
// The list of names is a sorted sequence of NUL-terminated file names.
// The initial entry in the list corresponds to file #0,
// the next to file #1, and so on.  The list ends with an
// empty name ("\x00").
//
// The list of posting lists are a sequence of posting lists.
// Each posting list has the form:
//
// 	trigram [3]
// 	deltas [v]...
//
// The trigram gives the 3 byte trigram that this list describes.  The
// delta list is a sequence of varint-encoded deltas between file
// IDs, ending with a zero delta.  For example, the delta list [2,5,1,1,0]
// encodes the file ID list 1, 6, 7, 8.  The delta list [0] would
// encode the empty file ID list, but empty posting lists are usually
// not recorded at all.  The list of posting lists ends with an entry
// with trigram "\xff\xff\xff" and a delta list consisting a single zero.
//
// The indexes enable efficient random access to the lists.  The name
// index is a sequence of 4-byte big-endian values listing the byte
// offset in the name list where each name begins.  The posting list
// index is a sequence of index entries describing each successive
// posting list.  Each index entry has the form:
//
// 	trigram [3]
// 	file count [4]
// 	offset [4]
//
// Index entries are only written for the non-empty posting lists,
// so finding the posting list for a specific trigram requires a
// binary search over the posting list index.  In practice, the majority
// of the possible trigrams are never seen, so omitting the missing
// ones represents a significant storage savings.
//
// The trailer has the form:
//
// 	offset of path list [4]
// 	offset of name list [4]
// 	offset of posting lists [4]
// 	offset of name index [4]
// 	offset of posting list index [4]
// 	"\ncsearch trailr\n"

use std::collections::BTreeSet;
use std::path::Path;
use std::io;
use std::fmt;
use std::fmt::Debug;
use std::io::Cursor;

use consts::TRAILER_MAGIC;
use memmap::{Mmap, Protection};
use byteorder::{BigEndian, ReadBytesExt};
use libvarint;

use regexp::{Query, QueryOperation};
use super::search;

pub const POST_ENTRY_SIZE: usize = 3 + 4 + 4;

/// Simple alias for an ID representing a filename in the Index.
pub type FileID = u32;


/// Representation of an Index
///
/// ```rust
/// # extern crate regex_syntax;
/// # extern crate libcsearch;
/// # use libcsearch::reader::IndexReader;
/// # use libcsearch::regexp::RegexInfo;
/// # use regex_syntax::Expr;
/// # use std::io;
/// # fn main() { foo(); }
/// # fn foo() -> io::Result<()> {
/// let expr = Expr::parse(r"Pattern").unwrap();
/// let q = RegexInfo::new(expr).unwrap().query;
///
/// let idx = try!(IndexReader::open("foo.txt"));
///
/// let matching_file_ids = idx.query(q);
///
/// for each in matching_file_ids.into_inner() {
///    println!("filename = {}", idx.name(each));
/// }
/// # Ok(())
/// # }
/// ```
pub struct IndexReader {
    data: Mmap,
    path_data: u32,
    name_data: u32,
    pub post_data: u32,
    name_index: usize,
    pub post_index: usize,
    pub num_name: usize,
    pub num_post: usize,
}

impl Debug for IndexReader {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f,
               "({}, {}, {}, {}, {}, {}, {}",
               self.path_data,
               self.name_data,
               self.post_data,
               self.name_index,
               self.post_index,
               self.num_name,
               self.num_post)
    }
}

fn extract_data_from_mmap(data: &Mmap, offset: usize) -> u32 {
    unsafe {
        let mut buf = Cursor::new(&data.as_slice()[offset..offset + 4]);
        buf.read_u32::<BigEndian>().unwrap()
    }
}


impl IndexReader {
    fn extract_data(&self, offset: usize) -> u32 {
        unsafe {
            let mut buf = Cursor::new(&self.data.as_slice()[offset..offset + 4]);
            buf.read_u32::<BigEndian>().unwrap()
        }
    }
    /// Open an index file from path
    ///
    /// ```no_run
    /// # use libcsearch::reader::IndexReader;
    /// # use std::io;
    /// # fn foo() -> io::Result<()> {
    /// let idx = try!(IndexReader::open("foo.txt"));
    /// # Ok(())
    /// # }
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<IndexReader> {
        Mmap::open_path(path, Protection::Read).map(|m| {
            let n = m.len() - (TRAILER_MAGIC.bytes().len()) - 5 * 4;
            let path_data = extract_data_from_mmap(&m, n);
            let name_data = extract_data_from_mmap(&m, n + 4);
            let post_data = extract_data_from_mmap(&m, n + 8);
            let name_index = extract_data_from_mmap(&m, n + 12) as usize;
            let post_index = extract_data_from_mmap(&m, n + 16) as usize;
            let num_name: usize = if post_index > name_index {
                let d = (post_index - name_index) / 4;
                if d == 0 {
                    0
                } else {
                    (d - 1) as usize
                }
            } else {
                0
            };
            let num_post = if n > (post_index as usize) {
                (n - (post_index as usize)) / (3 + 4 + 4)
            } else {
                0
            };
            IndexReader {
                data: m,
                path_data: path_data,
                name_data: name_data,
                post_data: post_data,
                name_index: name_index,
                post_index: post_index,
                num_name: num_name,
                num_post: num_post,
            }
        })
    }

    /// Takes a query and returns a list of matching file IDs.
    pub fn query<'a>(&'a self, query: Query) -> PostSet<'a> {
        // writeln!(io::stderr(), "query {:?}", query).unwrap();
        match query.operation {
            QueryOperation::None => PostSet::new(self),
            QueryOperation::All => PostSet {
                index: self,
                list: (0..self.num_name as u32).collect::<BTreeSet<FileID>>()
            },
            QueryOperation::And => {
                // writeln!(io::stderr(), "AND {:?}", query.trigram).unwrap();
                let mut trigram_it = query.trigram
                    .into_iter()
                    .map(|t| {
                        (t[0] as u32) << 16 | (t[1] as u32) << 8 | (t[2] as u32)
                    });
                let mut sub_iter = query.sub.into_iter().map(|q| self.query(q));
                let post_set = if let Some(i) = trigram_it.next() {
                    let s = PostSet::new(self).or(i).unwrap_or(PostSet::new(self));
                    Some(trigram_it.fold(s, |a, b| a.and(b).unwrap_or(PostSet::new(self))))
                } else {
                    sub_iter.next()
                };
                let post_set = if let Some(ps) = post_set {
                    ps
                } else {
                    return PostSet::new(self);
                };
                let sub_iter = sub_iter.map(|q| q.into_inner());
                sub_iter.fold(post_set, |mut a, b| {
                    a.list = &a.list & &b;
                    a
                })
            },
            QueryOperation::Or => {
                // writeln!(io::stderr(), "OR {:?}", query.trigram).unwrap();
                let trigram_it = query.trigram
                    .into_iter()
                    .map(|t| {
                        (t[0] as u32) << 16 | (t[1] as u32) << 8 | (t[2] as u32)
                    });
                let post_set = trigram_it.fold(PostSet::new(self), |a, b| {
                    a.or(b).unwrap_or(PostSet::new(self))
                });
                // writeln!(io::stderr(), "post set size = {:?}", post_set.list.len()).unwrap();
                query.sub.into_iter().map(|q| self.query(q).into_inner())
                    .fold(post_set, |mut a, b| {
                        a.list.extend(b.into_iter());
                        a
                    })
            }
        }
    }

    /// Returns the size of the index
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns the index as a slice
    pub unsafe fn as_slice(&self) -> &[u8] {
        self.data.as_slice()
    }

    /// Returns all indexed paths
    pub fn indexed_paths(&self) -> Vec<String> {
        let mut paths = Vec::new();
        let mut offset = self.path_data as usize;
        loop {
            let s = self.extract_string_at(offset);
            if s.len() == 0 {
                break;
            }
            offset += s.len() + 1;
            paths.push(s);
        }
        paths
    }

    /// Returns the name of a file identified by file_id
    pub fn name(&self, file_id: FileID) -> String {
        let file_id_usize = file_id as usize;
        let offset = self.extract_data(self.name_index + 4 * file_id_usize);
        self.extract_string_at((self.name_data + offset) as usize)
    }

    pub fn list_at(&self, offset: usize) -> (u32, u32, u32) {
        let d: &[u8] = unsafe {
            let s = self.data.as_slice();
            let (_, right_side) = s.split_at(self.post_index + offset);
            let (d, _) = right_side.split_at(POST_ENTRY_SIZE);
            d
        };
        let tri_val = (d[0] as u32) << 16 | (d[1] as u32) << 8 | (d[2] as u32);
        let count = {
            let (_, mut right) = d.split_at(3);
            right.read_u32::<BigEndian>().unwrap()
        };
        let offset = {
            let (_, mut right) = d.split_at(3 + 4);
            right.read_u32::<BigEndian>().unwrap()
        };
        (tri_val, count, offset)
    }

    /// Extract a null-terminated string from `offset`
    fn extract_string_at(&self, offset: usize) -> String {
        let mut index = 0;
        let mut s = String::new();
        unsafe {
            let sl = self.as_slice();
            while sl[offset + index] != 0 {
                s.push(sl[offset + index] as char);
                index += 1;
            }
            s
        }
    }

    /// Returns the offset and size of a list
    fn find_list(&self, trigram: u32) -> (isize, u32) {
        let d: &[u8] = unsafe {
            let s = self.data.as_slice();
            let (_, right_side) = s.split_at(self.post_index);
            let (d, _) = right_side.split_at(POST_ENTRY_SIZE * self.num_post);
            d
        };
        let result = search::search(self.num_post, |i| {
            let i_scaled = i * POST_ENTRY_SIZE;
            let tri_val = (d[i_scaled] as u32) << 16 | (d[i_scaled + 1] as u32) << 8 |
                          (d[i_scaled + 2] as u32);
            tri_val >= trigram
        });
        if result >= self.num_post {
            return (0, 0);
        }
        let result_scaled: usize = result * POST_ENTRY_SIZE;
        let tri_val = (d[result_scaled] as u32) << 16 | (d[result_scaled + 1] as u32) << 8 |
                      (d[result_scaled + 2] as u32);
        if tri_val != trigram {
            return (0, 0);
        }
        let count = {
            let (_, mut right) = d.split_at(result_scaled + 3);
            right.read_i32::<BigEndian>().unwrap() as isize
        };
        let offset = {
            let (_, mut right) = d.split_at(result_scaled + 3 + 4);
            right.read_u32::<BigEndian>().unwrap()
        };
        (count, offset)
    }
}

#[derive(Debug)]
pub struct PostReader<'a, 'b> {
    index: &'a IndexReader,
    count: isize,
    offset: u32,
    fileid: i64,
    d: &'a [u8],
    restrict: &'b Option<BTreeSet<u32>>,
}

impl<'a, 'b> PostReader<'a, 'b> {
    pub fn new(index: &'a IndexReader,
               trigram: u32,
               restrict: &'b Option<BTreeSet<u32>>)
               -> Option<Self> {
        let (count, offset) = index.find_list(trigram);
        if count == 0 {
            return None;
        }
        let view = unsafe {
            let v = index.data.as_slice();
            let split_point = (index.post_data as usize) + (offset as usize) + 3;
            v.split_at(split_point).1
        };
        Some(PostReader {
            index: index,
            count: count,
            offset: offset,
            fileid: -1,
            d: view,
            restrict: restrict,
        })
    }
    pub fn and(index: &'a IndexReader,
               list: BTreeSet<u32>,
               trigram: u32,
               restrict: &'b Option<BTreeSet<u32>>)
               -> BTreeSet<u32> {
        if let Some(mut r) = Self::new(index, trigram, restrict) {
            let mut h = BTreeSet::new();
            while r.next() {
                let fileid = r.fileid;
                if list.contains(&(fileid as u32)) {
                    h.insert(fileid as u32);
                }
            }
            h
        } else {
            BTreeSet::new()
        }
    }
    pub fn or(index: &'a IndexReader,
              list: BTreeSet<u32>,
              trigram: u32,
              restrict: &'b Option<BTreeSet<u32>>)
              -> BTreeSet<u32> {
        if let Some(mut r) = Self::new(index, trigram, restrict) {
            let mut h = list;
            while r.next() {
                h.insert(r.fileid as u32);
            }
            h
        } else {
            BTreeSet::new()
        }
    }
    pub fn list(index: &'a IndexReader, trigram: u32, restrict: &Option<BTreeSet<u32>>) -> BTreeSet<u32> {
        if let Some(mut r) = Self::new(index, trigram, restrict) {
            let mut x = BTreeSet::<u32>::new();
            while r.next() {
                x.insert(r.fileid as u32);
            }
            x
        } else {
            BTreeSet::new()
        }
    }
    // FIXME: refactor either to use rust iterator or don't look like an iterator
    fn next(&mut self) -> bool {
        while self.count > 0 {
            self.count -= 1;
            let (delta, n) = libvarint::read_uvarint(self.d).unwrap();
            if n <= 0 || delta == 0 {
                panic!("corrupt index");
            }
            self.d = self.d.split_at(n as usize).1;
            self.fileid += delta as i64;
            let is_fileid_found = match *self.restrict {
                Some(ref r) if r.contains(&(self.fileid as u32)) => true,
                None => true,
                _ => false
            };
            if !is_fileid_found {
                continue;
            }
            return true;
        }
        // list should end with terminating 0 delta
        // FIXME: add bounds checking
        self.fileid = -1;
        return false;
    }
}

pub struct PostSet<'a> {
    index: &'a IndexReader,
    list: BTreeSet<u32>
}

impl<'a> PostSet<'a> {
    pub fn new(index: &'a IndexReader) -> Self {
        PostSet {
            index: index,
            list: BTreeSet::new()
        }
    }
    pub fn into_inner(self) -> BTreeSet<u32> { self.list }
    pub fn and(self, trigram: u32) -> Option<Self> {
        let (mut d, count) = unsafe {
            if let Some(tup) = Self::make_view(&self.index, trigram) {
                tup
            } else {
                return None;
            }
        };
        let mut fileid = -1;
        let mut h = BTreeSet::new();
        for _ in 0 .. count {
            let (delta, n) = libvarint::read_uvarint(d).unwrap();
            if n <= 0 || delta == 0 {
                panic!("corrupt index");
            }
            d = d.split_at(n as usize).1;
            fileid += delta as i64;
            if self.list.contains(&(fileid as u32)) {
                h.insert(fileid as u32);
            }
        }
        Some(PostSet {
            index: self.index,
            list: h
        })
    }
    pub fn or(mut self, trigram: u32) -> Option<Self> {
        let (mut d, count) = unsafe {
            if let Some(tup) = Self::make_view(&self.index, trigram) {
                tup
            } else {
                return Some(self);
            }
        };
        let mut fileid = -1;
        // writeln!(io::stderr(), "TRI 0x{:6x}: {}", trigram, count).unwrap();
        for _ in 0 .. count {
            let (delta, n) = libvarint::read_uvarint(d).unwrap();
            if n <= 0 || delta == 0 {
                panic!("corrupt index");
            }
            d = d.split_at(n as usize).1;
            fileid += delta as i64;
            self.list.insert(fileid as u32);
        }
        Some(self)
    }
    unsafe fn make_view(index: &'a IndexReader, trigram: u32) -> Option<(&'a [u8], usize)> {
        let (count, offset) = index.find_list(trigram);
        if count == 0 {
            // writeln!(io::stderr(), "TRI 0x{:6x}: 0", trigram).unwrap();
            return None;
        }
        let v = index.data.as_slice();
        let split_point = (index.post_data as usize) + (offset as usize) + 3;
        Some((v.split_at(split_point).1, count as usize))
    }
}
