#![allow(dead_code)]

extern crate byteorder;
extern crate memmap;
extern crate num;
extern crate regex;
extern crate regex_syntax;
extern crate varint;

mod index;

fn main() {
    println!("Running");
    let i = index::read::Index::open("/home/vernon/.csearchindex").unwrap();
    for each in i.indexed_paths() {
        println!("{}", each);
    }
    let expr_input = r"postingList|derp";
    let expr = regex_syntax::Expr::parse(expr_input.clone()).unwrap();
    let re = regex::Regex::new(expr_input.clone()).unwrap();
    let q = index::regexp::RegexInfo::new(&expr).query;

    let post = i.query(q, None);

    // TODO: used for file filtering
    // let file_ids = post.iter().filter(|&file_id| {
    //     let name = i.name(*file_id as usize);
    //     re.is_match(&name)
    // }).collect::<Vec<_>>();

    for file_id in post {
        let name = i.name(file_id as usize);
        println!("name = {}", name);
    }

}
