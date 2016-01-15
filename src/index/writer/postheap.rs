use std::fs::File;
use std::io::{self, Cursor};
use std::iter::Peekable;
use std::u64;
use std::vec;

use index::byteorder::{BigEndian, ReadBytesExt};
use index::memmap::{Mmap, Protection};
use index::profiling;

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
    pub fn len(&self) -> usize {
        self.ch.iter()
            .map(|i| i.size_hint().0)
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
        self.ch.push(ch.into_iter().peekable());
        Ok(())
    }
    pub fn add_mem(&mut self, v: Vec<PostEntry>) {
        let _frame = profiling::profile("PostHeap::add_mem");
        self.ch.push(v.into_iter().peekable());
    }
}

impl IntoIterator for PostHeap {
    type Item = PostEntry;
    type IntoIter = IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            inner: self
        }
    }
}

pub struct IntoIter {
    inner: PostHeap
}

impl IntoIter {
    pub fn new(inner: PostHeap) -> Self {
        IntoIter {
            inner: inner
        }
    }
}

impl Iterator for IntoIter {
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
        assert!(p.len() == 2);
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
        assert!(p.len() == v_comb.len());
        assert!(!p.is_empty());
        assert!(p.into_iter().collect::<Vec<_>>() == v_comb);
    }
}
