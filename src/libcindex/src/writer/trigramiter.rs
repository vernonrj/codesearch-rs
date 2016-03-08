use std::io::{self, Read, BufReader};

use super::error::{IndexResult, IndexError, IndexErrorKind};


/// Yields 24-bit trigrams of characters from a reader
///
/// The Iterator interface is implemented to allow iteration over the trigrams.
/// The `.next()` method has a slight quirk, in that if an error occurs, iteration
/// stops immediately and the error is stored in an internal variable that can be
/// queried by the `.error()` method. This is because this iteration is a fairly
/// tight loop, and flattening the return of the `.next()` method from
/// `Option<Result<u32>>` to `Option<u32>` resulted in a respectable speedup.
/// ```
pub struct TrigramReader<R: Read> {
    reader: io::Bytes<BufReader<R>>,
    current_value: u32,
    num_read: usize,

    inv_cnt: u64,
    max_invalid: u64,

    line_len: u64,
    max_line_len: u64,

    error: Option<IndexResult<()>>,
}

impl<R: Read> TrigramReader<R> {
    /// If an error occurred during reading, extracts it into an option
    pub fn take_error(&mut self) -> Option<IndexResult<()>> {
        self.error.take()
    }
    pub fn new(r: R, max_invalid: u64, max_line_len: u64) -> TrigramReader<R> {
        TrigramReader {
            reader: BufReader::with_capacity(16384, r).bytes(),
            current_value: 0,
            num_read: 0,
            inv_cnt: 0,
            max_invalid: max_invalid,
            line_len: 0,
            max_line_len: max_line_len,
            error: None,
        }
    }
    fn next_char(&mut self) -> Option<u8> {
        match self.reader.next() {
            Some(Err(e)) => {
                self.error = Some(Err(e.into()));
                None
            }
            Some(Ok(c)) => {
                self.num_read += 1;
                Some(c)
            }
            None => None,
        }
    }
}

impl<R: Read> Iterator for TrigramReader<R> {
    type Item = u32;
    fn next(&mut self) -> Option<Self::Item> {
        let c = match self.next_char() {
            Some(c) => c,
            None if self.num_read > 0 && self.num_read < 3 => {
                self.num_read = 0;
                return Some(self.current_value);
            }
            _ => return None,
        };
        self.current_value = ((1 << 24) - 1) & ((self.current_value << 8) | (c as u32));
        if self.num_read < 3 {
            return self.next();
        }

        let b1 = ((self.current_value >> 8) & 0xff) as u8;
        let b2 = (self.current_value & 0xff) as u8;
        if b1 == 0x00 || b2 == 0x00 {
            // Binary file. Skip
            self.error = Some(Err(IndexError::new(IndexErrorKind::BinaryDataPresent,
                                                  format!("Binary File. Bytes {:02x}{:02x} at \
                                                           offset {}",
                                                          b1,
                                                          b2,
                                                          self.num_read))));
            None
        } else if !valid_utf8(b1, b2) {
            // invalid utf8 data
            self.inv_cnt += 1;
            if self.inv_cnt > self.max_invalid {
                let e = IndexError::new(IndexErrorKind::HighInvalidUtf8Ratio,
                                        format!("High invalid UTF-8 ratio. total {} invalid: {} \
                                                 ratio: {}",
                                                self.num_read,
                                                self.inv_cnt,
                                                (self.inv_cnt as f64) / (self.num_read as f64)));
                self.error = Some(Err(e));
                None
            } else {
                // skip invalid character
                self.next()
            }
        } else if self.line_len > self.max_line_len {
            let e = IndexError::new(IndexErrorKind::LineTooLong,
                                    format!("Line too long ({} > {})",
                                            self.line_len,
                                            self.max_line_len));
            self.error = Some(Err(e));
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
    let c = TrigramReader::new("hello".as_bytes(), 0, 100).next().unwrap();
    let hel = ('h' as u32) << 16 | ('e' as u32) << 8 | ('l' as u32);
    assert_eq!(c, hel);
}

#[test]
pub fn test_trigram_iter() {
    let trigrams: Vec<u32> = TrigramReader::new("hello".as_bytes(), 0, 100).collect();
    let hel = ('h' as u32) << 16 | ('e' as u32) << 8 | ('l' as u32);
    let ell = ('e' as u32) << 16 | ('l' as u32) << 8 | ('l' as u32);
    let llo = ('l' as u32) << 16 | ('l' as u32) << 8 | ('o' as u32);
    assert_eq!(trigrams, vec![hel, ell, llo]);
}
