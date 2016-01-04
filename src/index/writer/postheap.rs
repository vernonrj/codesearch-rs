use std::fs::File;
use std::io::{self, Cursor};
use std::iter::Peekable;
use std::u64;
use std::vec;

use index::byteorder::{BigEndian, ReadBytesExt};
use index::memmap::{Mmap, Protection};

use super::NPOST;
use super::postentry::PostEntry;


pub struct PostHeap {
    ch: Vec<Peekable<vec::IntoIter<PostEntry>>>
}

impl PostHeap {
    pub fn new() -> PostHeap {
        PostHeap {
            ch: Vec::new()
        }
    }
    pub fn len(&self) -> usize { self.ch.len() }
    pub fn is_empty(&self) -> bool { self.ch.is_empty() }
    pub fn add_file(&mut self, f: &File) -> io::Result<()> {
        let m = try!(Mmap::open(f, Protection::Read));
        let mut bytes = Cursor::new(unsafe { m.as_slice() });
        let mut ch = Vec::with_capacity(NPOST);
        while let Ok(p) = bytes.read_u64::<BigEndian>() {
            ch.push(PostEntry(p));
        }
        self.ch.push(ch.into_iter().peekable());
        Ok(())
    }
    pub fn add_mem(&mut self, v: Vec<PostEntry>) {
        self.ch.push(v.into_iter().peekable());
    }
}

impl IntoIterator for PostHeap {
    type Item = PostEntry;
    type IntoIter = PostHeapIntoIter;
    fn into_iter(self) -> Self::IntoIter {
        PostHeapIntoIter {
            inner: self
        }
    }
}

pub struct PostHeapIntoIter {
    inner: PostHeap
}

impl PostHeapIntoIter {
    pub fn new(inner: PostHeap) -> Self {
        PostHeapIntoIter {
            inner: inner
        }
    }
}

impl Iterator for PostHeapIntoIter {
    type Item = PostEntry;
    fn next(&mut self) -> Option<Self::Item> {
        let min_idx = if self.inner.ch.is_empty() {
            return None;
        } else if self.inner.ch.len() == 1 {
            0
        } else {
            let mut min_idx = 0;
            let mut min_val = PostEntry(u64::MAX);
            for (each_idx, each_vec) in self.inner.ch.iter_mut().enumerate() {
                let each_val = if let Some(each_val) = each_vec.peek() {
                    each_val
                } else {
                    continue;
                };
                if *each_val < min_val {
                    min_val = *each_val;
                    min_idx = each_idx;
                }
            }
            min_idx
        };
        let min_val = self.inner.ch[min_idx].next().unwrap();
        if self.inner.ch[min_idx].peek().is_none() {
            self.inner.ch.remove(min_idx).last();
        }
        Some(min_val)
    }
}

