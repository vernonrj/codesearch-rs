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
use index::write::{get_offset, copy_file, IndexWriter};
use index;

use tempfile::TempFile;
use byteorder::{BigEndian, WriteBytesExt};

use std::io::{self, Write, Seek, SeekFrom, BufReader, BufWriter};
use std::u32;
use std::fs::File;

#[derive(Debug)]
pub struct IdRange {
    low: u32,
    high: u32,
    new: u32
}

pub struct PostMapReader<'a> {
    index: &'a Index,
    id_map: Vec<IdRange>,
    tri_num: u32,
    trigram: u32,
    count: u32,
    offset: u32,
    d: &'a [u8],
    old_id: u32,
    file_id: u32,
    i: usize
}

impl<'a> PostMapReader<'a> {
    pub fn new(index: &'a Index, id_map: Vec<IdRange>) -> PostMapReader<'a> {
        let s = unsafe { index.as_slice() };
        let mut p = PostMapReader {
            index: index,
            id_map: id_map,
            tri_num: 0,
            trigram: u32::MAX,
            count: 0,
            offset: 0,
            d: s,
            old_id: u32::MAX,
            file_id: 0,
            i: 0
        };
        p.load();
        p
    }
    pub fn next_trigram(&mut self) {
        self.tri_num += 1;
        self.load();
    }
    fn load(&mut self) {
        if self.tri_num >= (self.index.num_post as u32) {
            self.trigram = u32::MAX;
            self.count = 0;
            self.file_id = u32::MAX;
            return;
        }
        let (trigram, count, offset) = self.index.list_at((self.tri_num as usize) * POST_ENTRY_SIZE);
        self.trigram = trigram;
        self.count = count;
        self.offset = offset;
        if count == 0 {
            self.file_id = u32::MAX;
            return;
        }
        self.d = unsafe {
            let s = self.index.as_slice();
            let split_point = self.index.post_data + self.offset + 3;
            let (_, right_side) = s.split_at(split_point as usize);
            right_side
        };
        self.old_id = u32::MAX;
        self.i = 0;
    }
    pub fn next_id(&mut self) -> bool {
        while self.count > 0 {
            self.count -= 1;
            let (delta, n) = index::read_uvarint(self.d).unwrap();
            self.d = self.d.split_at(n as usize).1;
            self.old_id = self.old_id.wrapping_add(delta as u32);
            while self.i < self.id_map.len() && self.id_map[self.i].high <= self.old_id {
                self.i += 1;
            }
            if self.i >= self.id_map.len() {
                self.count = 0;
                break;
            }
            if self.old_id < self.id_map[self.i].low {
                continue;
            }
            self.file_id = self.id_map[self.i].new + self.old_id - self.id_map[self.i].low;
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
        let base = get_offset(&mut out).unwrap() as u32;
        PostDataWriter {
            out: out,
            post_index_file: BufWriter::with_capacity(256 << 10, TempFile::new().unwrap()),
            base: base,
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
            IndexWriter::write_trigram(&mut self.out, self.t).unwrap();
        }
        IndexWriter::write_uvarint(&mut self.out, id.wrapping_sub(self.last)).unwrap();
        self.last = id;
        self.count += 1;
    }
    pub fn end_trigram(&mut self) {
        if self.count == 0 {
            return;
        }
        IndexWriter::write_uvarint(&mut self.out, 0).unwrap();
        IndexWriter::write_trigram(&mut self.post_index_file, self.t).unwrap();
        IndexWriter::write_u32(&mut self.post_index_file, self.count).unwrap();
        IndexWriter::write_u32(&mut self.post_index_file, self.offset - self.base).unwrap();
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
        let limit = {
            let (l1, l2) = path.split_at(path.len()-1);
            assert!(l2.len() == 1);
            let l2_u = l2.chars().next().unwrap() as u8;
            l1.to_string() + &((l2_u + 1) as char).to_string()
        };
        while (i1 as usize) < ix1.num_name && ix1.name(i1 as u32) < limit {
            i1 += 1;
        }

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
        let hi = i2;
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
        panic!("merge: inconsistent index ({} < {})", i2, ix2.num_name);
    }
    let num_name = new;
    let mut ix3 = BufWriter::new(try!(File::create(dest)));
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
    let mut r1 = PostMapReader::new(&ix1, map1);
    let mut r2 = PostMapReader::new(&ix2, map2);

    let mut w = PostDataWriter::new(ix3);

    loop {
        if r1.trigram < r2.trigram {
            w.trigram(r1.trigram);
            while r1.next_id() {
                w.file_id(r1.file_id);
            }
            r1.next_trigram();
            w.end_trigram();
        } else if r2.trigram < r1.trigram {
            w.trigram(r2.trigram);
            while r2.next_id() {
                w.file_id(r2.file_id);
            }
            r2.next_trigram();
            w.end_trigram();
        } else {
            if r1.trigram == u32::MAX {
                break;
            }
            w.trigram(r1.trigram);
            r1.next_id();
            r2.next_id();
            while r1.file_id < u32::MAX || r2.file_id < u32::MAX {
                if r1.file_id < r2.file_id {
                    w.file_id(r1.file_id);
                    r1.next_id();
                } else if r2.file_id < r1.file_id {
                    w.file_id(r2.file_id);
                    r2.next_id();
                } else {
                    panic!("merge: inconsistent index");
                }
            }
            r1.next_trigram();
            r2.next_trigram();
            w.end_trigram();
        }
    }

    let mut ix3 = w.out;

    // Name index
    let name_index = try!(get_offset(&mut ix3));
    name_index_file.seek(SeekFrom::Start(0)).unwrap();
    copy_file(&mut ix3, &mut BufReader::new(name_index_file.into_inner().unwrap()));

    // Posting list index
    let post_index = get_offset(&mut ix3).unwrap();
    copy_file(&mut ix3, &mut BufReader::new(w.post_index_file.into_inner().unwrap()));
    
    trace!("path_data  = {}", path_data );
    trace!("name_data  = {}", name_data );
    trace!("post_data  = {}", post_data );
    trace!("name_index = {}", name_index); 
    trace!("post_index = {}", post_index); 


    IndexWriter::write_u32(&mut ix3, path_data as u32).unwrap();
    IndexWriter::write_u32(&mut ix3, name_data as u32).unwrap();
    IndexWriter::write_u32(&mut ix3, post_data as u32).unwrap();
    IndexWriter::write_u32(&mut ix3, name_index as u32).unwrap();
    IndexWriter::write_u32(&mut ix3, post_index as u32).unwrap();
    IndexWriter::write_string(&mut ix3, index::TRAILER_MAGIC).unwrap();
    Ok(())
}
