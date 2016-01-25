#![allow(dead_code)]
extern crate chrono;
extern crate tempfile;
extern crate byteorder;
extern crate num;
extern crate memmap;
extern crate regex;
extern crate regex_syntax;

extern crate hprof;


pub mod writer;
pub mod merge;
pub mod profiling;

use std::env;


pub fn csearch_index() -> String {
    env::var("CSEARCHINDEX")
        .or_else(|_| env::var("HOME").or_else(|_| env::var("USERPROFILE"))
                        .map(|s| s + &"/.csearchindex"))
        .expect("no valid path to index")
}


