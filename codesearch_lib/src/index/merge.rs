// Copyright 2015 Vernon Jones.
// Original code Copyright 2013 Manpreet Singh ( junkblocker@yahoo.com ). All rights reserved.
// Original code Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


// Merging indexes.
//
// To merge two indexes A and B (newer) into a combined index C:
//
// Load the path list from B and determine for each path the docid ranges
// that it will replace in A.
//
// Read A's and B's name lists together, merging them into C's name list.
// Discard the identified ranges from A during the merge.  Also during the merge,
// record the mapping from A's docids to C's docids, and also the mapping from
// B's docids to C's docids.  Both mappings can be summarized in a table like
//
//	10-14 map to 20-24
//	15-24 is deleted
//	25-34 maps to 40-49
//
// The number of ranges will be at most the combined number of paths.
// Also during the merge, write the name index to a temporary file as usual.
//
// Now merge the posting lists (this is why they begin with the trigram).
// During the merge, translate the docid numbers to the new C docid space.
// Also during the merge, write the posting list index to a temporary file as usual.
//
// Copy the name index and posting list index into C's index and write the trailer.
// Rename C's index onto the new index.

use index::read::{Index, POST_ENTRY_SIZE};
use index::write::{get_offset, IndexWriter};
use index;

use tempfile::TempFile;
use byteorder::{BigEndian, WriteBytesExt};
use varint::{VarintRead, VarintWrite};

use std::io::{self, Write, Seek, BufWriter, Cursor};
use std::ops::Deref;
use std::u32;
use std::fs::File;

pub struct IdRange {
    low: u32,
    high: u32,
    new: u32
}

pub struct PostMapReader {
    index: Index,
    tri_num: u32,
    trigram: u32,
    offset: u32,
    id_map: Vec<IdRange>
}

impl PostMapReader {
    pub fn new(index: Index, id_map: Vec<IdRange>) -> PostMapReader {
        PostMapReader {
            index: index,
            id_map: id_map,
            tri_num: 0,
            trigram: u32::MAX,
            offset: 0
        }
    }
    pub fn next_trigram(&mut self) -> Option<PostMapReaderSlice> {
        self.tri_num += 1;
        self.load()
    }
    fn load(&mut self) -> Option<PostMapReaderSlice> {
        if self.tri_num >= (self.index.num_post as u32) {
            self.trigram = u32::MAX;
            return None;
        }
        let (trigram, count, offset) = self.index.list_at((self.tri_num as usize) * POST_ENTRY_SIZE);
        self.trigram = trigram;
        self.offset = offset;
        if count == 0 {
            return None;
        }
        let view = unsafe {
            let s = self.index.as_slice();
            let split_point = self.index.post_data + self.offset + 3;
            let (_, right_side) = s.split_at(split_point as usize);
            Cursor::new(right_side.iter().cloned().collect::<Vec<_>>())
        };
        Some(PostMapReaderSlice {
            file_id: 0,
            count: count,
            d: view,
            old_id: u32::MAX,
            i: 0
        })
    }
}

pub struct PostMapReaderSlice {
    file_id: u32,
    count: u32,
    d: Cursor<Vec<u8>>,
    old_id: u32,
    i: usize
}

impl PostMapReaderSlice {
    pub fn next_id(&mut self, id_map: &Vec<IdRange>) -> bool {
        while self.count > 0 {
            self.count -= 1;
            let delta = self.d.read_unsigned_varint_32().unwrap();
            self.old_id += delta;
            while self.i < id_map.len() && id_map[self.i].high <= self.old_id {
                self.i += 1;
            }
            if self.i >= id_map.len() {
                self.count = 0;
                break;
            }
            if self.old_id < id_map[self.i].low {
                continue;
            }
            self.file_id = id_map[self.i].new + self.old_id - id_map[self.i].low;
            return true;
        }
        self.file_id = u32::MAX;
        return false;
    }
}

struct PostDataWriter<W: Write + Seek> {
    out: BufWriter<W>,
    post_index_file: BufWriter<TempFile>,
    base: u32,
    count: u32,
    offset: u32,
    last: u32,
    t: u32
}

impl<W: Write + Seek> PostDataWriter<W> {
    pub fn new(out: BufWriter<W>) -> PostDataWriter<W> {
        let mut out = out;
        let offset = get_offset(&mut out).unwrap() as u32;
        PostDataWriter {
            out: out,
            post_index_file: BufWriter::new(TempFile::new().unwrap()),
            base: offset,
            count: 0,
            offset: 0,
            last: 0,
            t: 0
        }
    }
    pub fn trigram(&mut self, t: u32) {
        self.offset = get_offset(&mut self.out).unwrap() as u32;
        self.count = 0;
        self.t = t;
        self.last = u32::MAX;
    }
    pub fn file_id(&mut self, id: u32) {
        if self.count == 0 {
            IndexWriter::write_trigram(&mut self.out, self.t);
        }
        let mut v = Cursor::new(Vec::<u8>::new());
        v.write_unsigned_varint_32(id - self.last);
        self.out.write(v.into_inner().deref());
        self.last = id;
        self.count += 1;
    }
    pub fn end_trigram(&mut self) {
        if self.count == 0 {
            return;
        }
        let mut v = Cursor::new(Vec::<u8>::new());
        v.write_unsigned_varint_32(0);
        self.out.write(v.into_inner().deref());
        IndexWriter::write_trigram(&mut self.out, self.t);
        IndexWriter::write_u32(&mut self.out, self.count);
        IndexWriter::write_u32(&mut self.out, self.offset - self.base);
    }
}

pub fn merge(dest: String, src1: String, src2: String) -> io::Result<()> {
    let ix1 = try!(Index::open(src1));
    let ix2 = try!(Index::open(src2));
    let paths1 = ix1.indexed_paths();
    let paths2 = ix2.indexed_paths();

    let mut i1: u32 = 0;
    let mut i2: u32 = 0;
    let mut new: u32 = 0;
    let mut map1 = Vec::<IdRange>::new();
    let mut map2 = Vec::<IdRange>::new();
    for path in &paths2 {
        let old = i1;
        while (i1 as usize) < ix1.num_name && ix1.name(i1 as u32) < *path {
            i1 += 1;
        }
        let mut lo = i1;
        let limit = path.clone();
        while (i1 as usize) < ix1.num_name && ix1.name(i1 as u32) <= limit {
            i1 += 1;
        }
        let mut hi = i1;

        // Record range before the shadow
        if old < lo {
            map1.push(IdRange { low: old, high: lo, new: new});
            new += lo - old;
        }
        
        // Determine range defined by this path.
        // Because we are iterating over the ix2 paths,
        // there can't be gaps, so it must start at i2.
        if (i2 as usize) < ix2.num_name && ix2.name(i2) < *path {
            panic!("merge: inconsistent index");
        }
        lo = i2;
        while (i2 as usize) < ix2.num_name && ix2.name(i2) < limit {
            i2 += 1;
        }
        hi = i2;
        if lo < hi {
            map2.push(IdRange { low: lo, high: hi, new: new });
            new += hi - lo;
        }
    }

    if (i1 as usize) < ix1.num_name {
        map1.push(IdRange { low: i1, high: ix1.num_name as u32, new: new });
        new += (ix1.num_name as u32) - i1;
    }
    if (i2 as usize) < ix2.num_name {
        panic!("merge: inconsistent index");
    }
    let num_name = new;
    let mut ix3 = BufWriter::new(try!(TempFile::new()));
    IndexWriter::write_string(&mut ix3, index::MAGIC).unwrap();

    let path_data = try!(get_offset(&mut ix3));
    let mut mi1 = 0;
    let mut mi2 = 0;
    let mut last = "\0".to_string(); // not a prefix of anything

    while mi1 < paths1.len() && mi2 < paths2.len() {
        let p = if mi2 >= paths2.len() || mi1 < paths1.len() && paths1[mi1] <= paths2[mi2] {
            let p = paths1[mi1].clone();
            mi1 += 1;
            p
        } else {
            let p = paths2[mi2].clone();
            mi2 += 1;
            p
        };
        if p.starts_with(&last) {
            continue;
        }
        last = p.clone();
        IndexWriter::write_string(&mut ix3, &p).unwrap();
        IndexWriter::write_string(&mut ix3, "\0").unwrap();
    }
    IndexWriter::write_string(&mut ix3, "\0").unwrap();

    // Merged list of names
    let name_data = try!(get_offset(&mut ix3));
    let mut name_index_file = BufWriter::new(try!(TempFile::new()));

    new = 0;
    mi1 = 0;
    mi2 = 0;

    while new < num_name {
        if mi1 < map1.len() && map1[mi1].new == new {
            for i in map1[mi1].low .. map1[mi1].high {
                let name = ix1.name(i);
                let new_offset: u32 = try!(get_offset(&mut ix3)) as u32;
                name_index_file.write_u32::<BigEndian>(new_offset - (name_data as u32)).unwrap();
                IndexWriter::write_string(&mut ix3, &name).unwrap();
                IndexWriter::write_string(&mut ix3, "\0").unwrap();
                new += 1;
            }
            mi1 += 1;
        } else if mi2 < map2.len() && map2[mi2].new == new {
            for i in map2[mi2].low .. map2[mi2].high {
                let name = ix2.name(i);
                let new_offset: u32 = try!(get_offset(&mut ix3)) as u32;
                name_index_file.write_u32::<BigEndian>(new_offset - (name_data as u32)).unwrap();
                IndexWriter::write_string(&mut ix3, &name).unwrap();
                IndexWriter::write_string(&mut ix3, "\0").unwrap();
                new += 1;
            }
            mi2 += 1;
        } else {
            panic!("merge: inconsistent index");
        }
    }
    if ((new*4) as u64) != try!(get_offset(&mut name_index_file)) {
        panic!("merge: inconsistent index");
    }
    name_index_file.write_u32::<BigEndian>(try!(get_offset(&mut ix3)) as u32).unwrap();

    // Merged list of posting lists.
    let post_data = try!(get_offset(&mut ix3));
    let mut r1 = PostMapReader::new(ix1, map1);
    let mut r1_slice = r1.load().expect("failed to get PostMapReaderSlice for r1");
    let mut r2 = PostMapReader::new(ix2, map2);
    let mut r2_slice = r2.load().expect("failed to get PostMapReaderSlice for r2");

    let mut w = PostDataWriter::new(ix3);

    loop {
        if r1.trigram < r2.trigram {
            w.trigram(r1.trigram);
            while r1_slice.next_id(&r1.id_map) {
                w.file_id(r1_slice.file_id);
            }
            r1_slice = r1.next_trigram().expect("failed to get next PostMapReaderSlice for r1");
            w.end_trigram();
        } else if r2.trigram < r1.trigram {
            w.trigram(r2.trigram);
            while r2_slice.next_id(&r2.id_map) {
                w.file_id(r2_slice.file_id);
            }
            r2_slice = r2.next_trigram().expect("failed to get next PostMapReaderSlice for r2");
            w.end_trigram();
        } else {
            if r1.trigram == u32::MAX {
                break;
            }
            w.trigram(r1.trigram);
            r1_slice.next_id(&r1.id_map);
            r2_slice.next_id(&r2.id_map);
            while r1_slice.file_id < u32::MAX || r2_slice.file_id < u32::MAX {
                if r1_slice.file_id < r2_slice.file_id {
                    w.file_id(r1_slice.file_id);
                    r1_slice.next_id(&r1.id_map);
                } else if r2_slice.file_id < r1_slice.file_id {
                    w.file_id(r2_slice.file_id);
                    r2_slice.next_id(&r2.id_map);
                } else {
                    panic!("merge: inconsistent index");
                }
            }

            r1_slice = r1.next_trigram().expect("failed to get next PostMapReaderSlice for r1");
            r2_slice = r2.next_trigram().expect("failed to get next PostMapReaderSlice for r2");
            w.end_trigram();
        }
    }
    
    unimplemented!();
}
