// Implements a wrapper for the Posting List

use std::iter::Peekable;
use std::num::Wrapping;
use std::u32;
use std::slice;
use std::vec;


use super::postentry::PostEntry;

#[derive(Debug)]
pub struct PostingList {
    trigram: u32,
    deltas: DeltaList
}

impl PostingList {
    pub fn new(trigram: u32) -> Self {
        PostingList {
            trigram: trigram,
            deltas: DeltaList::default()
        }
    }
    pub fn push_file(&mut self, file_id: u32) {
        self.deltas.push(file_id);
    }
    pub fn trigram(&self) -> u32 { self.trigram }
    pub fn iter_deltas<'a>(&'a self) -> DeltaIter<'a> { self.deltas.iter() }
    pub fn delta_len(&self) -> usize { self.deltas.len() }
    pub fn aggregate_from<I>(peekable: &mut Peekable<I>) -> Option<Self>
        where I: Iterator<Item=PostEntry>
    {
        let mut plist = if let Some(pentry) = peekable.next() {
            PostingList {
                trigram: pentry.trigram(),
                deltas: DeltaList::new(vec![pentry.file_id()])
            }
        } else {
            return None;
        };
        loop {
            if peekable.peek().map(|p| p.trigram() == plist.trigram).unwrap_or(false) {
                let pentry: PostEntry = peekable.next().unwrap();
                plist.push_file(pentry.file_id());
            } else {
                break;
            }
        }
        Some(plist)
    }
}




#[derive(Debug, Default)]
pub struct DeltaList {
    file_ids: Vec<u32>
}

impl DeltaList {
    pub fn new(file_ids: Vec<u32>) -> Self {
        DeltaList {
            file_ids: file_ids
        }
    }
    pub fn push(&mut self, file_id: u32) {
        self.file_ids.push(file_id);
    }
    pub fn len(&self) -> usize { self.file_ids.len() }
    pub fn iter<'a>(&'a self) -> DeltaIter<'a> { DeltaIter::new(self.file_ids.iter()) }
}

impl IntoIterator for DeltaList {
    type Item = u32;
    type IntoIter = IntoDeltaIter;
    fn into_iter(self) -> Self::IntoIter {
        IntoDeltaIter::new(self.file_ids.into_iter())
    }
}





struct Transformer {
    last_value: u32
}

impl Transformer {
    pub fn new() -> Self {
        Transformer {
            last_value: u32::MAX
        }
    }
    pub fn next_value(&mut self, value: u32) -> u32 {
        let Wrapping(diff) = Wrapping(value) - Wrapping(self.last_value);
        self.last_value = value;
        diff
    }
}

pub struct DeltaIter<'a> {
    inner: slice::Iter<'a, u32>,
    delta: Transformer,
    wrote_trailing_zero: bool
}

impl<'a> DeltaIter<'a> {
    pub fn new(inner: slice::Iter<'a, u32>) -> Self {
        DeltaIter {
            inner: inner,
            delta: Transformer::new(),
            wrote_trailing_zero: false
        }
    }
}

impl<'a> Iterator for DeltaIter<'a> {
    type Item = u32;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
            .map(|i| self.delta.next_value(*i))
            .or_else(|| {
                if !self.wrote_trailing_zero {
                    self.wrote_trailing_zero = true;
                    Some(0)
                } else {
                    None
                }
            })
    }
}

pub struct IntoDeltaIter {
    inner: vec::IntoIter<u32>,
    delta: Transformer,
    wrote_trailing_zero: bool
}

impl IntoDeltaIter {
    pub fn new(inner: vec::IntoIter<u32>) -> Self {
        IntoDeltaIter {
            inner: inner,
            delta: Transformer::new(),
            wrote_trailing_zero: false
        }
    }
}

impl Iterator for IntoDeltaIter {
    type Item = u32;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
            .map(|i| self.delta.next_value(i))
            .or_else(|| {
                if !self.wrote_trailing_zero {
                    self.wrote_trailing_zero = true;
                    Some(0)
                } else {
                    None
                }
            })
    }
}


#[test]
fn test_into_iter() {
    let p = DeltaList::new(vec![1, 6, 7, 8]);
    let result = p.into_iter().collect::<Vec<_>>();
    println!("result = {:?}", result);
    assert!(result == vec![2, 5, 1, 1, 0]);
}

#[test]
fn test_iter() {
    let p = DeltaList::new(vec![1, 6, 7, 8]);
    println!("vals = {:?}", p.iter().collect::<Vec<_>>());
    assert!(p.iter().collect::<Vec<_>>() == vec![2, 5, 1, 1, 0]);
    // make sure ownership works correctly
    assert!(p.iter().collect::<Vec<_>>() == vec![2, 5, 1, 1, 0]);
}

#[test]
fn test_from_aggregate() {
    let entries = vec![PostEntry::new(0x001100, 1),
                       PostEntry::new(0x001100, 6),
                       PostEntry::new(0x001122, 7)];
    let mut it = entries.into_iter().peekable();
    let plist = PostingList::aggregate_from(&mut it).unwrap();

    // ensure PostingList only took the trigram matches
    assert!(it.next() == Some(PostEntry::new(0x001122, 7)));
    assert!(it.next().is_none());

    println!("{:?}", plist.iter_deltas().collect::<Vec<_>>());
    assert!(plist.iter_deltas().collect::<Vec<_>>() == vec![2, 5, 0]);
}
