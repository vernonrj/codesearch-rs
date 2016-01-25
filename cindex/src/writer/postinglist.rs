// Implements a wrapper for the Posting List

use std::iter::{self, Chain, Scan, Once, Peekable};
use std::num::Wrapping;
use std::u32;

use super::postentry::PostEntry;



pub struct TakeWhilePeek<'a, I: 'a + Iterator<Item=PostEntry>> {
    trigram: u32,
    it: &'a mut Peekable<I>
}

impl<'a, I: 'a + Iterator<Item=PostEntry>> TakeWhilePeek<'a, I> {
    pub fn new(it: &'a mut Peekable<I>) -> Option<Self> {
        let t = if let Some(entry) = it.peek() {
            entry.trigram()
        } else {
            return None;
        };
        Some(TakeWhilePeek {
            trigram: t,
            it: it
        })
    }
    pub fn trigram(&self) -> u32 { self.trigram }
}

impl<'a, I: 'a + Iterator<Item=PostEntry>> Iterator for TakeWhilePeek<'a, I> {
    type Item = PostEntry;
    fn next(&mut self) -> Option<Self::Item> {
        let t = if let Some(entry) = self.it.peek() {
            entry.trigram()
        } else {
            return None;
        };
        if t == self.trigram {
            self.it.next()
        } else {
            None
        }
    }
}

pub fn to_diffs<'a, I: 'a + Iterator<Item=u32>>(it: I)
        -> Chain<Scan<I, u32, fn(&mut u32, u32) -> Option<u32>>, Once<u32>>
{
    let f: fn(&mut u32, u32) -> Option<u32> = transform;
    it.scan(u32::MAX, f).chain(iter::once(0))
}

fn transform(delta: &mut u32, x: u32) -> Option<u32> {
    let Wrapping(diff) = Wrapping(x) - Wrapping(*delta);
    *delta = x;
    Some(diff)
}


#[test]
fn test_into_iter() {
    let v = vec![PostEntry::new(100, 1),
                 PostEntry::new(100, 6),
                 PostEntry::new(100, 7),
                 PostEntry::new(100, 8)];
    let result = {
        let mut it = v.clone().into_iter().peekable();
        let p = TakeWhilePeek::new(&mut it).unwrap();
        p.into_iter().collect::<Vec<_>>()
    };
    println!("result = {:?}", result);
    assert_eq!(result, v);
}

#[test]
fn test_to_diffs() {
    let v = vec![PostEntry::new(100, 1),
                 PostEntry::new(100, 6),
                 PostEntry::new(100, 7),
                 PostEntry::new(100, 8)];
    let mut it = v.clone().into_iter().peekable();
    let chunk = TakeWhilePeek::new(&mut it).unwrap();
    let result = to_diffs(chunk.map(|p| p.file_id())).collect::<Vec<_>>();
    assert_eq!(result, vec![2, 5, 1, 1, 0]);
}

#[test]
fn test_dont_consume_next_trigram() {
    let v = vec![PostEntry::new(100, 1),
                 PostEntry::new(100, 6),
                 PostEntry::new(101, 8)];
    let mut it = v.clone().into_iter().peekable();
    {
        let chunk = TakeWhilePeek::new(&mut it).unwrap();
        let result = to_diffs(chunk.map(|p| p.file_id())).collect::<Vec<_>>();
        assert_eq!(result, vec![2, 5, 0]);
    }
    assert_eq!(it.collect::<Vec<_>>(), vec![PostEntry::new(101, 8)]);
}
