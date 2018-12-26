// Copyright 2016 Vernon Jones
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#[macro_use]
extern crate clap;
extern crate log;

extern crate libcustomlogger;
extern crate libcsearch;

use libcsearch::reader::{POST_ENTRY_SIZE, IndexReader};
use libcsearch::regexp::Query;

use std::collections::BTreeSet;
use std::env;
use std::io::{self, Write};

fn main() {
    libcustomlogger::init(log::LogLevelFilter::Info).unwrap();

    let matches = clap::App::new("cinspect")
        .version(&crate_version!()[..])
        .author("Vernon Jones <vernonrjones@gmail.com>")
        .about("helper tool to inspect index files")
        .arg(clap::Arg::with_name("INDEX_FILE")
            .long("indexpath")
            .takes_value(true)
            .help("use specified INDEX_FILE as the index path. overwrites $CSEARCHINDEX."))
        .arg(clap::Arg::with_name("files")
            .long("files")
            .short("f")
            .help("list indexed files"))
        .arg(clap::Arg::with_name("with-trigram")
            .long("with-trigram")
            .short("t")
            .help("list all files that contain trigram")
            .takes_value(true))
        .arg(clap::Arg::with_name("postinglist")
            .long("posting-list")
            .help("Prints the posting list"))
        .get_matches();

    // possibly override the csearchindex
    matches.value_of("INDEX_FILE").map(|p| {
        env::set_var("CSEARCHINDEX", p);
    });

    let index_path = libcsearch::csearch_index();
    let idx = IndexReader::open(index_path).unwrap();


    if matches.is_present("files") {
        print_indexed_files(&idx);
    }

    if matches.is_present("postinglist") {
        match dump_posting_list(&idx) {
            Ok(_) => (),
            Err(_) => return,
        }
    }

    if let Some(t) = matches.value_of("with-trigram") {
        let t_num = u32::from_str_radix(t, 10).unwrap();
        let mut h: Option<BTreeSet<u32>> = None;
        let file_ids = libcsearch::reader::PostReader::list(&idx, t_num, &mut h);
        println!("{:?}", file_ids);
    }

}

fn print_indexed_files(idx: &IndexReader) {
    let post = idx.query(Query::all());
    for each_fileid in post.into_inner() {
        println!("{}: {}", each_fileid, idx.name(each_fileid));
    }
}

fn dump_posting_list(idx: &IndexReader) -> io::Result<()> {
    let d: &[u8] = unsafe {
        idx.as_slice()
            .split_at(idx.post_index)
            .1
            .split_at(POST_ENTRY_SIZE * idx.num_post)
            .0
    };
    for i in 0..idx.num_post {
        try!(writeln!(&mut std::io::stdout(),
                      "{} {} {}",
                      d[i * POST_ENTRY_SIZE],
                      d[i * POST_ENTRY_SIZE + 1],
                      d[i * POST_ENTRY_SIZE + 2]));
    }
    Ok(())
}
