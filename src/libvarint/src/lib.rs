// Ported from Go's binary.varint lib.
// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file./

use std::io::{self, Write};

pub fn read_uvarint(b: &[u8]) -> Result<(u64, u64), u64> {
    let mut x: u64 = 0;
    let mut s: usize = 0;
    for (i, b) in b.iter().enumerate() {
        if *b < 0x80 {
            if i > 9 || i == 9 && *b > 1 {
                return Err((i + 1) as u64);
            } else {
                return Ok((x | ((*b as u64) << s), ((i + 1) as u64)));
            }
        }
        x |= ((b & 0x7f) as u64) << s;
        s += 7;
    }
    Err(0)
}


pub fn write_uvarint<W: Write>(writer: &mut W, x: u32) -> io::Result<usize> {
    let mut bytes_written = 0;
    let mut x = x;
    while x >= 0x80 {
        try!(writer.write(&mut [((x & 0xff) as u8) | 0x80]));
        x >>= 7;
        bytes_written += 1;
    }
    try!(writer.write(&mut [(x & 0xff) as u8]));
    Ok(bytes_written + 1)
}
