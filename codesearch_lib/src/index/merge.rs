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

use index::read::Index;
use index::write::{get_offset, IndexWriter};
use index;

use tempfile::TempFile;
use byteorder::{BigEndian, WriteBytesExt};

use std::io::{self, BufWriter};

pub struct IdRange {
    low: u32,
    high: u32,
    new: u32
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

    
    unimplemented!();
}
