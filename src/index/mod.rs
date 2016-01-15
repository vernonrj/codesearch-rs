#![allow(dead_code)]
extern crate chrono;
extern crate tempfile;
extern crate byteorder;
extern crate num;
extern crate memmap;
extern crate regex;
extern crate regex_syntax;

extern crate hprof;


pub mod reader;
pub mod writer;
pub mod merge;
pub mod profiling;
mod varint;

use std::env;

pub const MAGIC: &'static str        = "csearch index 1\n";
pub const TRAILER_MAGIC: &'static str = "\ncsearch trailr\n";

pub fn csearch_index() -> String {
    env::var("CSEARCHINDEX")
        .or_else(|_| env::var("HOME").or_else(|_| env::var("USERPROFILE"))
                        .map(|s| s + &"/.csearchindex"))
        .expect("no valid path to index")
}


