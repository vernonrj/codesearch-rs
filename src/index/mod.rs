#![allow(dead_code)]
extern crate tempfile;
extern crate byteorder;
extern crate num;
extern crate memmap;
extern crate regex;
extern crate regex_syntax;
extern crate varint;

pub mod read;
pub mod write;
pub mod merge;
pub mod regexp;
mod sparseset;
mod search;

use std::env;

pub const MAGIC: &'static str        = "csearch index 1\n";
pub const TRAILER_MAGIC: &'static str = "\ncsearch trailr\n";

pub fn csearch_index() -> String {
    env::var("CSEARCHINDEX")
        .or_else(|_| env::var("HOME").or_else(|_| env::var("USERPROFILE"))
                        .map(|s| s + &"/.csearchindex"))
        .expect("no valid path to index")
}


// Ported from Go's binary.varint lib.
// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file./
pub fn read_uvarint(b: &[u8]) -> Result<(u64, u64), u64> {
    let mut x: u64 = 0;
    let mut s: usize = 0;
    for (i, b) in b.iter().enumerate() {
        if *b < 0x80 {
            if i > 9 || i == 9 && *b > 1 {
                return Err((i+1) as u64);
            } else {
                return Ok((x | ((*b as u64) << s), ((i + 1) as u64)));
            }
        }
        x |= ((b & 0x7f) as u64) << s;
        s += 7;
    }
    Err(0)
}

