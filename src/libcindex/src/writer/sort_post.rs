// Original Code Copyright 2011 The Go Authors.  All rights reserved.
// Original Code Copyright 2013 Manpreet Singh ( junkblocker@yahoo.com ). All rights reserved.
//
// Copyright 2016 Vernon Jones. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

use std::mem;

use super::postentry::PostEntry;
use libprofiling;


// sortPost sorts the postentry list.
// The list is already sorted by fileid (bottom 32 bits)
// and the top 8 bits are always zero, so there are only
// 24 bits to sort.  Run two rounds of 12-bit radix sort.
const K: usize = 12;


pub fn sort_post(post: &mut Vec<PostEntry>) {
    let _frame = libprofiling::profile("sort_post");
    let mut sort_tmp = Vec::<PostEntry>::with_capacity(post.len());
    unsafe { sort_tmp.set_len(post.len()) };
    let mut sort_n = [0; 1<<K];
    for p in post.iter() {
        let r = p.trigram() & ((1<<K) - 1);
        sort_n[r as usize] += 1;
    }
    let mut tot = 0;
    for count in sort_n.iter_mut() {
        let val = *count;
        *count = tot;
        tot += val;
    }
    for p in post.iter() {
        let r = (p.trigram() & ((1<<K) - 1)) as usize;
        let o = sort_n[r];
        sort_n[r] += 1;
        sort_tmp[o] = *p;
    }
    mem::swap(post, &mut sort_tmp);

    sort_n = [0; 1<<K];
    for p in post.iter() {
        let r = ((p.value() >> (32+K)) & ((1<<K) - 1)) as usize;
        sort_n[r] += 1;
    }
    tot = 0;
    for count in sort_n.iter_mut() {
        let val = *count;
        *count = tot;
        tot += val;
    }
    for p in post.iter() {
        let r = ((p.value() >> (32+K)) & ((1<<K) - 1)) as usize;
        let o = sort_n[r];
        sort_n[r] += 1;
        sort_tmp[o] = *p;
    }
    mem::swap(post, &mut sort_tmp);
}

#[test]
fn test_sort() {
    let mut v = vec![PostEntry::new(5, 0), PostEntry::new(1, 0), PostEntry::new(10, 0),
                     PostEntry::new(4, 1), PostEntry::new(4, 5), PostEntry::new(5, 10)];
    let mut v_1 = v.clone();
    v_1.sort();
    sort_post(&mut v);
    assert_eq!(v, v_1);
}
