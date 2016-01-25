extern crate byteorder;
extern crate memmap;
extern crate num;
extern crate regex;
extern crate regex_syntax;

extern crate consts;
extern crate libvarint;

pub mod grep;
pub mod reader;

use std::env;


pub fn csearch_index() -> String {
    env::var("CSEARCHINDEX")
        .or_else(|_| env::var("HOME").or_else(|_| env::var("USERPROFILE"))
                        .map(|s| s + &"/.csearchindex"))
        .expect("no valid path to index")
}
