// Copyright 2015 Vernon Jones.
// Original code Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#![allow(dead_code)]
use std::mem;
use std::slice;
use std::vec;

const MAX_SIZE: u32 = 1 << 24;
const STARTING_DENSE_SIZE: usize = 10000;

pub struct SparseSet {
    sparse: Vec<u32>,
    dense: Vec<u32>
}

impl SparseSet {
    pub fn new() -> SparseSet {
        let mut v = Vec::with_capacity(MAX_SIZE as usize);
        unsafe { v.set_len(MAX_SIZE as usize) };
        SparseSet {
            sparse: v,
            dense: Vec::with_capacity(STARTING_DENSE_SIZE)
        }
    }
    pub fn insert(&mut self, x: u32) {
        let v = self.sparse[x as usize];
        let n = self.dense.len();
        if (v as usize) < n && self.dense[v as usize] == x {
            return;
        }
        self.sparse[x as usize] = n as u32;
        self.dense.push(x);
    }
    pub fn contains(&self, x: u32) -> bool {
        let v = self.sparse[x as usize];
        v < (self.dense.len() as u32) && self.dense[v as usize] == x
    }
    pub fn clear(&mut self) {
        self.dense.clear();
    }
    pub fn len(&self) -> usize { self.dense.len() }
    pub fn is_empty(&self) -> bool { self.dense.is_empty() }
    pub fn into_vec(self) -> Vec<u32> { self.dense }
    pub fn take_dense(&mut self) -> Vec<u32> {
        let mut dense_new = Vec::with_capacity(STARTING_DENSE_SIZE);
        mem::swap(&mut dense_new, &mut self.dense);
        dense_new
    }
}

impl IntoIterator for SparseSet {
    type Item = u32;
    type IntoIter = vec::IntoIter<u32>;

    fn into_iter(self) -> Self::IntoIter {
        self.dense.into_iter()
    }
}

impl<'a> IntoIterator for &'a SparseSet {
    type Item = &'a u32;
    type IntoIter = slice::Iter<'a, u32>;

    fn into_iter(self) -> slice::Iter<'a, u32> {
        self.dense.iter()
    }

}

#[test]
fn test_init() {
    let s = SparseSet::new();
    assert!(s.len() == 0);
    assert!(s.is_empty());
}

#[test]
fn test_insert() {
    let mut s = SparseSet::new();
    s.insert(5);
    assert!(s.len() == 1);
}

#[test]
fn test_insert_mult_unique() {
    let mut s = SparseSet::new();
    for each in 0 .. 10 {
        s.insert(each);
    }
    assert!(s.len() == 10);
}

#[test]
fn test_insert_overlapping() {
    let mut s = SparseSet::new();
    s.insert(1);
    s.insert(1);
    assert!(s.len() == 1);
}

#[test]
fn test_contains() {
    let mut s = SparseSet::new();
    s.insert(1);
    assert!(s.contains(1));
}

#[test]
fn test_into_vec() {
    let mut s = SparseSet::new();
    s.insert(5);
    s.insert(10);
    s.insert(0);
    assert!(s.into_vec() == vec![5, 10, 0]);
}

#[test]
fn test_into_iter() {
    let mut s = SparseSet::new();
    s.insert(5);
    s.insert(0);
    s.insert(30);
    s.insert(4);
    assert!(s.into_iter().collect::<Vec<u32>>() == vec![5, 0, 30, 4]);
}
