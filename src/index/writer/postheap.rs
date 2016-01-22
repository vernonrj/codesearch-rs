use std::fs::File;
use std::io::{self, Cursor};
use std::vec;

use index::byteorder::{BigEndian, ReadBytesExt};
use index::memmap::{Mmap, Protection};
use index::profiling;

use super::NPOST;
use super::postentry::PostEntry;

struct PostChunk {
    e: PostEntry,
    m: vec::IntoIter<PostEntry>,
    size: usize
}

impl PostChunk {
    pub fn new(v: Vec<PostEntry>) -> Option<PostChunk> {
        let size = v.len();
        if size == 0 {
            None
        } else {
            let mut m = v.into_iter();
            let e = m.next().unwrap();
            Some(PostChunk {
                e: e,
                m: m,
                size: size
            })
        }
    }
    pub fn peek<'a>(&'a self) -> &'a PostEntry {
        debug_assert!(!self.is_empty());
        &self.e
    }
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
    pub fn len(&self) -> usize {
        self.size
    }
}

impl Iterator for PostChunk {
    type Item = PostEntry;
    fn next(&mut self) -> Option<Self::Item> {
        if self.is_empty() {
            return None;
        }
        let result = self.e;
        if let Some(c) = self.m.next() {
            self.e = c;
        }
        self.size -= 1;
        Some(result)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size, Some(self.size))
    }
}

pub struct PostHeap {
    ch: Vec<PostChunk>
}

impl PostHeap {
    pub fn new() -> PostHeap {
        PostHeap {
            ch: Vec::new()
        }
    }
    pub fn len(&self) -> usize {
        self.ch.iter()
               .map(PostChunk::len)
               .fold(0, |a, b| a + b)
    }
    pub fn is_empty(&self) -> bool { self.ch.is_empty() }
    pub fn add_file(&mut self, f: &File) -> io::Result<()> {
        let _frame = profiling::profile("PostHeap::add_file");
        let m = try!(Mmap::open(f, Protection::Read));
        let mut bytes = Cursor::new(unsafe { m.as_slice() });
        let mut ch = Vec::with_capacity(NPOST);
        while let Ok(p) = bytes.read_u64::<BigEndian>() {
            ch.push(PostEntry(p));
        }
        self.add_mem(ch);
        Ok(())
    }
    pub fn add_mem(&mut self, v: Vec<PostEntry>) {
        let _frame = profiling::profile("PostHeap::add_mem");
        if let Some(p) = PostChunk::new(v) {
            self.add(p);
        }
    }
    fn add(&mut self, ch: PostChunk) {
        if !ch.is_empty() {
            self.push(ch);
        }
    }
    fn push(&mut self, ch: PostChunk) {
        let n = self.ch.len();
        self.ch.push(ch);
        if self.ch.len() >= 2 {
            self.sift_up(n);
        }
    }
    fn sift_down(&mut self, mut i: usize) {
        let mut ch = &mut self.ch;
        let len = ch.len();
        loop {
            let j1 = 2*i + 1;
            if j1 >= len {
                break;
            }
            let j2 = j1 + 1;
            let j = if j2 < len && ch[j1].e >= ch[j2].e {
                j2
            } else {
                j1
            };
            if ch[i].e < ch[j].e {
                break;
            }
            ch.swap(i, j);
            i = j;
        }
    }
    fn sift_up(&mut self, mut j: usize) {
        while j > 0 {
            let i = (j - 1) / 2;
            if (i == j) || self.ch[i].e < self.ch[j].e {
                break;
            }
            self.ch.swap(i, j);
            j = i;
        }
    }
}

impl IntoIterator for PostHeap {
    type Item = PostEntry;
    type IntoIter = IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        IntoIter::new(self)
    }
}

pub struct IntoIter {
    inner: PostHeap,
    place: usize
}

impl IntoIter {
    pub fn new(inner: PostHeap) -> Self {
        IntoIter {
            inner: inner,
            place: 0
        }
    }
}

impl Iterator for IntoIter {
    type Item = PostEntry;
    fn next(&mut self) -> Option<Self::Item> {
        if self.place < self.inner.ch.len() {
            let e = self.inner.ch[self.place].next();
            if self.inner.ch[self.place].is_empty() {
                self.place += 1;
            } else {
                self.inner.sift_down(self.place);
            }
            e
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::postentry::PostEntry;

    #[test]
    fn test_postheap_build() {
        let p = PostHeap::new();
        assert!(p.len() == 0);
        assert!(p.is_empty());
    }

    #[test]
    fn test_postheap_add_mem() {
        let mut p = PostHeap::new();
        p.add_mem(vec![PostEntry::new(0, 32),
                       PostEntry::new(5, 32)]);
        assert_eq!(p.len(), 2);
        assert!(!p.is_empty());
    }

    #[test]
    fn test_postheap_iter_single() {
        let mut p = PostHeap::new();
        let v = vec![PostEntry::new(0, 32), PostEntry::new(5, 32)];
        p.add_mem(v.clone());
        assert!(p.into_iter().collect::<Vec<_>>() == v);
    }

    #[test]
    fn test_postheap_iter_mult() {
        let v1 = vec![PostEntry::new(0, 32), PostEntry::new(5, 32)];
        let v2 = vec![PostEntry::new(2, 32), PostEntry::new(6, 32)];
        let v_comb = {
            let mut v = v1.clone();
            v.extend(v2.iter().cloned());
            v.sort();
            v
        };
        let mut p = PostHeap::new();
        p.add_mem(v1.clone());
        p.add_mem(v2.clone());
        assert_eq!(p.len(), v_comb.len());
        assert!(!p.is_empty());
        assert!(p.into_iter().collect::<Vec<_>>() == v_comb);
    }
}
