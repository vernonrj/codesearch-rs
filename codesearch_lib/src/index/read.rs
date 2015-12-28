// Copyright 2015 Vernon Jones.
// Original code Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Index format.
//
// An index stored on disk has the format:
//
//	"csearch index 1\n"
//	list of paths
//	list of names
//	list of posting lists
//	name index
//	posting list index
//	trailer
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
//	trigram [3]
//	deltas [v]...
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
//	trigram [3]
//	file count [4]
//	offset [4]
//
// Index entries are only written for the non-empty posting lists,
// so finding the posting list for a specific trigram requires a
// binary search over the posting list index.  In practice, the majority
// of the possible trigrams are never seen, so omitting the missing
// ones represents a significant storage savings.
//
// The trailer has the form:
//
//	offset of path list [4]
//	offset of name list [4]
//	offset of posting lists [4]
//	offset of name index [4]
//	offset of posting list index [4]
//	"\ncsearch trailr\n"

use std::path::Path;
use std::io;
use std::fmt;
use std::fmt::Debug;

use memmap::{Mmap, Protection};
use std::io::Cursor;
use byteorder::{BigEndian, ReadBytesExt};
use varint::VarintRead;

use index::regexp::{Query, QueryOperation};
use index::search;

static TRAILER_MAGIC: &'static str = "\ncsearch trailr\n";
pub const POST_ENTRY_SIZE: usize = 3 + 4 + 4;

/// Simple alias for an ID representing a filename in the Index.
pub type FileID = u32;


/// Representation of an Index
///
/// ```rust
/// # extern crate regex_syntax;
/// # extern crate codesearch_lib;
/// # use codesearch_lib::index::read::Index;
/// # use codesearch_lib::index::regexp::RegexInfo;
/// # use regex_syntax::Expr;
/// # use std::io;
/// # fn main() { foo(); }
/// # fn foo() -> io::Result<()> {
/// let expr = Expr::parse(r"Pattern").unwrap();
/// let q = RegexInfo::new(&expr).query;
///
/// let idx = try!(Index::open("foo.txt"));
///
/// let matching_file_ids = idx.query(q, None);
///
/// for each in matching_file_ids {
///    println!("filename = {}", idx.name(each));
/// }
/// # Ok(())
/// # }
/// ```
pub struct Index {
    data: Mmap,
    path_data: u32,
    name_data: u32,
    pub post_data: u32,
    name_index: usize,
    post_index: usize,
    pub num_name: usize,
    pub num_post: usize
}

impl Debug for Index {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "({}, {}, {}, {}, {}, {}, {}",
               self.path_data, self.name_data, self.post_data,
               self.name_index, self.post_index, self.num_name, self.num_post)
    }
}

fn extract_data_from_mmap(data: &Mmap, offset: usize) -> u32 {
    unsafe {
        let mut buf = Cursor::new(&data.as_slice()[ offset .. offset + 4]);
        buf.read_u32::<BigEndian>().unwrap()
    }
}


impl Index {
    fn extract_data(&self, offset: usize) -> u32 {
        unsafe {
            let mut buf = Cursor::new(&self.data.as_slice()[ offset .. offset + 4]);
            buf.read_u32::<BigEndian>().unwrap()
        }
    }
    /// Open an index file from path
    ///
    /// ```no_run
    /// # use codesearch_lib::index::read::Index;
    /// # use std::io;
    /// # fn foo() -> io::Result<()> {
    /// let idx = try!(Index::open("foo.txt"));
    /// # Ok(())
    /// # }
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Index> {
        Mmap::open_path(path, Protection::Read)
            .map(|m| {
                let n = m.len() - (TRAILER_MAGIC.bytes().len()) - 5*4;
                let path_data = extract_data_from_mmap(&m, n);
                let name_data = extract_data_from_mmap(&m, n + 4);
                let post_data = extract_data_from_mmap(&m, n + 8);
                let name_index = extract_data_from_mmap(&m, n + 12) as usize;
                let post_index = extract_data_from_mmap(&m, n + 16) as usize;
                let num_name: usize = if post_index > name_index {
                    let d = (post_index - name_index) / 4;
                    if d == 0 { 0 } else { (d - 1) as usize }
                } else {
                    0
                };
                let num_post = if n > (post_index as usize) {
                    (n - (post_index as usize)) / (3 + 4 + 4)
                } else {
                    0
                };
                Index {
                    data: m,
                    path_data: path_data,
                    name_data: name_data,
                    post_data: post_data,
                    name_index: name_index,
                    post_index: post_index,
                    num_name: num_name,
                    num_post: num_post
                }
        })
    }

    /// Takes a query and returns a list of matching file IDs.
    pub fn query(&self, query: Query, mut restrict: Option<Vec<FileID>>) -> Vec<FileID> {
        match query.operation {
            QueryOperation::None => Vec::new(),
            QueryOperation::All => {
                if restrict.is_some() {
                    return restrict.unwrap();
                }
                let mut v = Vec::<u32>::new();
                for idx in 0 .. self.num_name {
                    v.push(idx as FileID);
                }
                v
            },
            QueryOperation::And => {
                let mut m_v: Option<Vec<FileID>> = None;
                for trigram in query.trigram {
                    let bytes = trigram.as_bytes();
                    let tri_val = (bytes[0] as u32) << 16
                                | (bytes[1] as u32) << 8
                                | (bytes[2] as u32);
                    if m_v.is_none() {
                        m_v = Some(PostReader::list(&self, tri_val, &mut restrict));
                    } else {
                        m_v = Some(PostReader::and(&self, m_v.unwrap(), tri_val, &mut restrict));
                    }
                    if let Some(v) = m_v {
                        if v.is_empty() {
                            return v;
                        } else {
                            m_v = Some(v);
                        }
                    }
                }
                for sub in query.sub {
                    // if m_v.is_none() {
                    //     m_v = restrict;
                    // }
                    let v = self.query(sub, m_v);
                    if v.len() == 0 {
                        return v;
                    }
                    m_v = Some(v);
                }
                return m_v.unwrap_or(Vec::new());
            },
            QueryOperation::Or => {
                let mut m_v = None;
                for trigram in query.trigram {
                    let bytes = trigram.as_bytes();
                    let tri_val = (bytes[0] as u32) << 16
                                | (bytes[1] as u32) << 8
                                | (bytes[2] as u32);
                    if m_v.is_none() {
                        m_v = Some(PostReader::list(&self, tri_val, &mut restrict));
                    } else {
                        m_v = Some(PostReader::or(&self, m_v.unwrap(), tri_val, &mut restrict));
                    }
                }
                for sub in query.sub {
                    let list1 = self.query(sub, restrict.clone());
                    m_v = Some(merge_or(m_v.unwrap_or(Vec::new()), list1))
                }
                return m_v.unwrap_or(Vec::new());
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
        let tri_val = (d[0] as u32) << 16
                    | (d[1] as u32) << 8
                    | (d[2] as u32);
        let count = {
            let (_, mut right) = d.split_at(3);
            right.read_u32::<BigEndian>().unwrap()
        };
        let offset = {
            let (_, mut right) = d.split_at(3+4);
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
                s.push(sl[offset+index] as char);
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
            let tri_val = (d[i_scaled] as u32) << 16
                        | (d[i_scaled+1] as u32) << 8
                        | (d[i_scaled+2] as u32);
            tri_val >= trigram
        });
        if result >= self.num_post {
            return (0, 0);
        }
        let result_scaled: usize = result * POST_ENTRY_SIZE;
        let tri_val = (d[result_scaled] as u32) << 16
                    | (d[result_scaled+1] as u32) << 8
                    | (d[result_scaled+2] as u32);
        if tri_val != trigram {
            return (0, 0);
        }
        let count = {
            let (_, mut right) = d.split_at(result_scaled+3);
            right.read_i32::<BigEndian>().unwrap() as isize
        };
        let offset = {
            let (_, mut right) = d.split_at(result_scaled+3+4);
            right.read_u32::<BigEndian>().unwrap()
        };
        (count, offset)
    }
}

fn merge_or(l1: Vec<u32>, l2: Vec<u32>) -> Vec<u32> {
    let mut l = Vec::new();
    let mut i = 0;
    let mut j = 0;
    while i < l1.len() || j < l2.len() {
		if j == l2.len() || i < l1.len() && l1[i] < l2[j] {
			l.push(l1[i]);
			i += 1;
        } else if i == l1.len() || (j < l2.len() && l1[i] > l2[j]) {
			l.push(l2[j]);
			j += 1;
        } else if l1[i] == l2[j] {
			l.push(l1[i]);
			i += 1;
			j += 1;
		}
	}
	return l;
}

#[derive(Debug)]
struct PostReader<'a, 'b> {
    index: &'a Index,
    count: isize,
    offset: u32,
    fileid: i64,
    d: Cursor<Vec<u8>>,
    restrict: &'b mut Option<Vec<u32>>
}

impl<'a, 'b> PostReader<'a, 'b> {
    pub fn new(index: &'a Index,
               trigram: u32,
               restrict: &'b mut Option<Vec<u32>>) -> Option<Self>
    {
        let (count, offset) = index.find_list(trigram);
        if count == 0 {
            return None;
        }
        let view = unsafe {
            let v = index.data.as_slice();
            let split_point = (index.post_data as usize) + (offset as usize) + 3;
            let (_, v1) = v.split_at(split_point);
            Cursor::new(v1.iter().cloned().collect::<Vec<_>>())
        };
        Some(PostReader {
            index: index,
            count: count,
            offset: offset,
            fileid: -1,
            d: view,
            restrict: restrict
        })
    }
    pub fn and(index: &'a Index,
               list: Vec<u32>,
               trigram: u32,
               restrict: &'b mut Option<Vec<u32>>) -> Vec<u32>
    {
        if let Some(mut r) = Self::new(index, trigram, restrict) {
            let mut v = Vec::new();
            let mut i = 0;
            while r.next() {
                let fileid = r.fileid;
                while i < list.len() && (list[i] as i64) < fileid {
                    i += 1;
                }
                if i < list.len() && (list[i] as i64) == fileid {
                    assert!(fileid >= 0);
                    v.push(fileid as u32);
                    i += 1;
                }
            }
            v
        } else {
            Vec::new()
        }
    }
    pub fn or(index: &'a Index,
              list: Vec<u32>,
              trigram: u32,
              restrict: &'b mut Option<Vec<u32>>) -> Vec<u32>
    {
        if let Some(mut r) = Self::new(index, trigram, restrict) {
            let mut v = Vec::new();
            let mut i = 0;
            while r.next() {
                let fileid = r.fileid;
                while i < list.len() && (list[i] as i64) < fileid {
                    v.push(list[i] as u32);
                    i += 1;
                }
                v.push(fileid as u32);
                if i < list.len() && (list[i] as i64) == fileid {
                    i += 1;
                }
            }
            v.extend(&list[i ..]);
            v
        } else {
            Vec::new()
        }
    }
    pub fn list(index: &'a Index, trigram: u32, restrict: &mut Option<Vec<u32>>) -> Vec<u32> {
        if let Some(mut r) = Self::new(index, trigram, restrict) {
            let mut x = Vec::<u32>::new();
            while r.next() {
                x.push(r.fileid as u32);
            }
            x
        } else {
            Vec::new()
        }
    }
    // TODO: refactor either to use rust iterator or don't look like an iterator
    fn next(&mut self) -> bool {
        while self.count > 0 {
            self.count -= 1;
            let delta = self.d.read_unsigned_varint_32().unwrap();
            self.fileid += delta as i64;
            let mut is_fileid_found = true;
            if let Some(ref mut r) = *self.restrict {
                let mut i = 0;
                while i < r.len() && (r[i] as i64) < self.fileid {
                    i += 1;
                }
                *r = r.split_off(i);
                if r.is_empty() || (r[0] as i64) != self.fileid {
                    is_fileid_found = false;
                }
            }
            if !is_fileid_found {
                continue
            }
            return true;
        }
        // list should end with terminating 0 delta
        // TODO: add bounds checking
        self.fileid = -1;
        return false;
    }
}

