use std::io::{self, Read, BufRead};
use std::cmp;

use super::error::{IndexResult, IndexError, IndexErrorKind};

/// A slight tweak of Rust's BufReader
struct SharedBuffer {
    inner: Vec<u8>
}

impl SharedBuffer {
    pub fn new(cap: usize) -> Self {
        SharedBuffer {
            inner: vec![0; cap]
        }
    }
    pub fn open<'a, R: Read>(&'a mut self, reader: R) -> SharedBufferReader<'a, R> {
        SharedBufferReader::new(reader, self)
    }
    pub fn len(&self) -> usize { self.inner.len() }
}

struct SharedBufferReader<'a, R: Read> {
    inner: R,
    shared: &'a mut SharedBuffer,
    pos: usize,
    cap: usize
}

impl<'a, R: Read> SharedBufferReader<'a, R> {
    fn new(inner: R, shared: &'a mut SharedBuffer) -> Self {
        SharedBufferReader {
            inner: inner,
            shared: shared,
            pos: 0,
            cap: 0
        }
    }
}


impl<'a, R: Read> Read for SharedBufferReader<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos == self.cap && buf.len() >= self.shared.len() {
            return self.inner.read(buf);
        }
        let nread = {
            let mut rem = try!(self.fill_buf());
            try!(rem.read(buf))
        };
        self.consume(nread);
        Ok(nread)
    }
}

impl<'a, R: Read> BufRead for SharedBufferReader<'a, R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.pos == self.cap {
            self.cap = try!(self.inner.read(&mut self.shared.inner));
            self.pos = 0;
        }
        Ok(&self.shared.inner[self.pos..self.cap])
    }
    fn consume(&mut self, amt: usize) {
        self.pos = cmp::min(self.pos + amt, self.cap);
    }
}



pub struct TrigramReader {
    inner: SharedBuffer
}

impl TrigramReader {
    pub fn new() -> Self { TrigramReader { inner: SharedBuffer::new(16384) } }
    pub fn open<'a, R: Read>(&'a mut self,
                             r: R,
                             max_invalid: u64,
                             max_line_len: u64) -> TrigramIter<'a, R> {
        TrigramIter::new(&mut self.inner, r, max_invalid, max_line_len)
    }
}

pub struct TrigramIter<'a, R: Read> {
    reader: io::Bytes<SharedBufferReader<'a, R>>,
    current_value: u32,
    num_read: usize,
    inv_cnt: u64,
    max_invalid: u64,
    line_len: u64,
    max_line_len: u64,
    error: Option<IndexResult<()>>
}

impl<'a, R: Read> TrigramIter<'a, R> {
    pub fn take_error(&mut self) -> Option<IndexResult<()>> {
        self.error.take()
    }
    fn new(shared: &'a mut SharedBuffer,
               r: R,
               max_invalid: u64,
               max_line_len: u64) -> TrigramIter<'a, R> {
        TrigramIter {
            reader: shared.open(r).bytes(),
            current_value: 0,
            num_read: 0,
            inv_cnt: 0,
            max_invalid: max_invalid,
            line_len: 0,
            max_line_len: max_line_len,
            error: None
        }
    }
    fn next_char(&mut self) -> Option<u8> {
        match self.reader.next() {
            Some(Err(e)) => {
                self.error = Some(Err(e.into()));
                None
            },
            Some(Ok(c)) => {
                self.num_read += 1;
                Some(c)
            },
            None => None
        }
    }
}

impl<'a, R: Read> Iterator for TrigramIter<'a, R> {
    type Item = u32;
    fn next(&mut self) -> Option<Self::Item> {
        let c = match self.next_char() {
            Some(c) => c,
            _ => {
                return if self.num_read > 0 && self.num_read < 3 {
                    self.num_read = 0;
                    return Some(self.current_value);
                } else {
                    return None;
                };
            }
        };
        self.current_value = ((1 << 24) - 1) & ((self.current_value << 8) | (c as u32));
        if self.num_read < 3 {
            return self.next();
        } else {
            let b1 = (self.current_value >> 8) & 0xff;
            let b2 = self.current_value & 0xff;
            if b1 == 0x00 || b2 == 0x00 {
                // Binary file. Skip
                self.error = Some(Err(IndexError::new(IndexErrorKind::BinaryDataPresent,
                                                      format!("Binary File. Bytes {:02x}{:02x} at offset {}",
                                                              b1, b2, self.num_read))));
                None
            } else if !valid_utf8(b1 as u8, b2 as u8) {
                // invalid utf8 data
                self.inv_cnt += 1;
                if self.inv_cnt > self.max_invalid {
                    self.error = Some(Err(IndexError::new(IndexErrorKind::HighInvalidUtf8Ratio,
                                                          format!("High invalid UTF-8 ratio. total {} invalid: {} ratio: {}",
                                                                  self.num_read, self.inv_cnt,
                                                                  (self.inv_cnt as f64) / (self.num_read as f64)
                                                                 ))));
                    None
                } else {
                    return self.next();
                }
            } else if self.line_len > self.max_line_len {
                self.error = Some(Err(IndexError::new(IndexErrorKind::LineTooLong,
                                                      format!("Line too long ({} > {})",
                                                      self.line_len, self.max_line_len))));
                None
            } else {
                if c == ('\n' as u8) { 
                    self.line_len = 0;
                } else {
                    self.line_len += 1;
                }
                Some(self.current_value)
            }
        }
    }
}

fn valid_utf8(c1: u8, c2: u8) -> bool {
    if c1 < 0x80 {
        // 1-byte, must be followed by 1-byte or first of multi-byte
        (c2 < 0x80) || (0xc0 <= c2) && (c2 < 0xf8)
    } else if c1 < 0xc0 {
        // continuation byte, can be followed by nearly anything
        (c2 < 0xf8)
    } else if c1 < 0xf8 {
        // first of multi-byte, must be followed by continuation byte
        (0x80 <= c2) && (c2 < 0xc0)
    } else {
        false
    }
}

#[test]
fn test_trigram_iter_once() {
    let c = TrigramReader::new()
        .open("hello".as_bytes(), 0, 100).next().unwrap();
    let hel =   ('h' as u32) << 16
              | ('e' as u32) << 8
              | ('l' as u32);
    assert!(c.unwrap() == hel);
}

#[test]
pub fn test_trigram_iter() {
    let trigrams: Vec<u32> = TrigramReader::new()
        .open("hello".as_bytes(), 0, 100)
        .map(Result::unwrap)
        .collect();
    let hel =   ('h' as u32) << 16
              | ('e' as u32) << 8
              | ('l' as u32);
    let ell =   ('e' as u32) << 16
              | ('l' as u32) << 8
              | ('l' as u32);
    let llo =   ('l' as u32) << 16
              | ('l' as u32) << 8
              | ('o' as u32);
    assert!(trigrams == vec![hel,ell,llo]);
}
