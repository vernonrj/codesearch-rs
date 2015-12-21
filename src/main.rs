#![allow(dead_code)]

extern crate byteorder;
extern crate memmap;
extern crate regex;
extern crate regex_syntax;

mod index;

fn main() {
    println!("Running");
    let i = index::read::Index::open("/home/vernon/.csearchindex").unwrap();
    for each in i.indexed_paths() {
        println!("{}", each);
    }
    let expr = regex_syntax::Expr::parse("derp").unwrap();
    println!("{:?}", expr);
    let q = index::regexp::RegexInfo::new(&expr).query;
    println!("{:?}", q);
}
