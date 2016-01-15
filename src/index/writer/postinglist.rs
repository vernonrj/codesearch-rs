// Implements a wrapper for the Posting List

use std::iter::{self, Chain, Once, Scan, Peekable};
use std::num::Wrapping;
use std::u32;
use std::vec;


use super::postentry::PostEntry;
use profiling;

#[derive(Debug, Clone)]
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
    pub fn into_deltas(self) -> IntoDeltaIter { self.deltas.into_iter() }
    pub fn delta_len(&self) -> usize { self.deltas.len() }
    pub fn aggregate_from<I>(peekable: &mut Peekable<I>) -> Option<Self>
        where I: Iterator<Item=PostEntry>
    {
        let _frame = profiling::profile("PostingList::aggregate_from");
        let mut plist = if let Some(pentry) = peekable.next() {
            PostingList {
                trigram: pentry.trigram(),
                deltas: DeltaList::new(vec![pentry.file_id()])
            }
        } else {
            return None;
        };
        let _frame_get_posting_list = profiling::profile("PostingList::aggregate_from: Get posting list");
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




#[derive(Debug, Default, Clone)]
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
}

impl IntoIterator for DeltaList {
    type Item = u32;
    type IntoIter = IntoDeltaIter;
    fn into_iter(self) -> Self::IntoIter {
        IntoDeltaIter::new(self.file_ids.into_iter())
    }
}





pub struct IntoDeltaIter {
    inner: Chain<Scan<vec::IntoIter<u32>, u32, fn(&mut u32, u32) -> Option<u32>>, Once<u32>>
}

impl IntoDeltaIter {
    pub fn new(inner: vec::IntoIter<u32>) -> Self {
        let f: fn(&mut u32, u32) -> Option<u32> = transform;
        let c = inner.scan(u32::MAX, f).chain(iter::once(0));;
        IntoDeltaIter {
            inner: c
        }
    }
}

fn transform(delta: &mut u32, x: u32) -> Option<u32> {
    let Wrapping(diff) = Wrapping(x) - Wrapping(*delta);
    *delta = x;
    Some(diff)
}

impl Iterator for IntoDeltaIter {
    type Item = u32;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}


#[test]
fn test_into_iter() {
    let p = DeltaList::new(vec![1, 6, 7, 8]);
    let result = p.into_iter().collect::<Vec<_>>();
    println!("result = {:?}", result);
    assert_eq!(result, vec![2, 5, 1, 1, 0]);
}

#[test]
fn test_iter() {
    let p = DeltaList::new(vec![1, 6, 7, 8]);
    println!("vals = {:?}", p.clone().into_iter().collect::<Vec<_>>());
    assert_eq!(p.clone().into_iter().collect::<Vec<_>>(), vec![2, 5, 1, 1, 0]);
    // make sure ownership works correctly
    assert_eq!(p.into_iter().collect::<Vec<_>>(), vec![2, 5, 1, 1, 0]);
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

    println!("{:?}", plist.clone().into_deltas().collect::<Vec<_>>());
    assert_eq!(plist.into_deltas().collect::<Vec<_>>(), vec![2, 5, 0]);
}
