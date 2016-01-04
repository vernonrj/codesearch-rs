use std::io::{self, BufReader, Read};

use super::error::{IndexResult, IndexError, IndexErrorKind};

const MAX_LINE_LEN: u64 = 2000;

pub struct TrigramIter<R: Read> {
    reader: io::Bytes<BufReader<R>>,
    current_value: u32,
    num_read: usize,
    inv_cnt: u64,
    max_invalid: u64,
    line_len: u64
}

impl<R: Read> TrigramIter<R> {
    pub fn new(r: R, max_invalid: u64) -> TrigramIter<R> {
        TrigramIter {
            reader: BufReader::with_capacity(16384, r).bytes(),
            current_value: 0,
            num_read: 0,
            inv_cnt: 0,
            max_invalid: max_invalid,
            line_len: 0
        }
    }
    fn next_char(&mut self) -> io::Result<Option<u8>> {
        match self.reader.next() {
            Some(Err(e)) => Err(e),
            Some(Ok(c)) => {
                self.num_read += 1;
                Ok(Some(c))
            },
            None => Ok(None)
        }
    }
}

impl<R: Read> Iterator for TrigramIter<R> {
    type Item = IndexResult<u32>;
    fn next(&mut self) -> Option<Self::Item> {
        let c = match self.next_char() {
            Ok(Some(c)) => c,
            Ok(None) => {
                return if self.num_read > 0 && self.num_read < 3 {
                    self.num_read = 0;
                    return Some(Ok(self.current_value));
                } else {
                    return None;
                };
            }
            Err(_) => return None     // done with error
        };
        self.current_value = ((1 << 24) - 1) & ((self.current_value << 8) | (c as u32));
        if self.num_read < 3 {
            return self.next();
        } else {
            let b1 = (self.current_value >> 8) & 0xff;
            let b2 = self.current_value & 0xff;
            if b1 == 0x00 || b2 == 0x00 {
                // Binary file. Skip
                Some(Err(IndexError::new(IndexErrorKind::BinaryDataPresent,
                                         format!("Binary File. Bytes {:02x}{:02x} at offset {}",
                                                 b1, b2, self.num_read))))
            } else if !valid_utf8(b1 as u8, b2 as u8) {
                // invalid utf8 data
                self.inv_cnt += 1;
                if self.inv_cnt > self.max_invalid {
                    Some(Err(IndexError::new(IndexErrorKind::HighInvalidUtf8Ratio,
                                             format!("High invalid UTF-8 ratio. total {} invalid: {} ratio: {}",
                                                     self.num_read, self.inv_cnt,
                                                     (self.inv_cnt as f64) / (self.num_read as f64)
                                                     ))))
                } else {
                    return self.next();
                }
            } else if self.line_len > MAX_LINE_LEN {
                Some(Err(IndexError::new(IndexErrorKind::LineTooLong,
                                         format!("Very long lines ({})", self.line_len))))
            } else {
                if c == ('\n' as u8) { 
                    self.line_len = 0;
                } else {
                    self.line_len += 1;
                }
                Some(Ok(self.current_value))
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
    let c = TrigramIter::new("hello".as_bytes(), 0).next().unwrap();
    let hel =   ('h' as u32) << 16
              | ('e' as u32) << 8
              | ('l' as u32);
    assert!(c.unwrap() == hel);
}

#[test]
pub fn test_trigram_iter() {
    let trigrams: Vec<u32> = TrigramIter::new("hello".as_bytes(), 0)
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
