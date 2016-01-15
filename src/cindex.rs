// Copyright 2015 Vernon Jones.
// Original code Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

extern crate chrono;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;

mod customlogger;
mod index;

use index::reader::read::IndexReader;
use index::writer::{IndexWriter, IndexErrorKind};
use index::profiling;
use log::LogLevelFilter;

use std::collections::HashSet;
use std::env;
use std::path::{Path, PathBuf};
use std::fs::{self, DirEntry, File, FileType};
use std::io::{self, Write, BufRead, BufReader};
#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;
use std::thread;
use std::sync::mpsc;
use std::ffi::OsString;
use std::str::FromStr;

fn walk_dir(dir: &Path, cb: &Fn(&DirEntry)) -> io::Result<()> {
    if try!(fs::metadata(dir)).is_dir() {
        for entry in try!(fs::read_dir(dir)) {
            let entry = try!(entry);
            if try!(fs::metadata(entry.path())).is_dir() {
                try!(walk_dir(&entry.path(), cb));
            } else {
                cb(&entry);
            }
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn is_regular_file(meta: FileType) -> bool {
    !meta.is_dir()
}

#[cfg(unix)]
fn is_regular_file(meta: FileType) -> bool {
    !meta.is_dir() && !meta.is_symlink()
        && !meta.is_fifo() && !meta.is_socket()
        && !meta.is_block_device() && !meta.is_char_device()
}

fn get_value_from_matches<F: FromStr>(matches: &clap::ArgMatches, name: &str) -> Option<F> {
    match matches.value_of(name) {
        Some(s) => {
            if let Ok(t) = s.parse::<F>() {
                Some(t)
            } else {
                panic!("can't convert value '{}' to number", s);
            }
        },
        _ => None
    }
}

fn main() {
    let matches = clap::App::new("cindex")
        .version(&crate_version!()[..])
        .author("Vernon Jones <vernonrjones@gmail.com> (original code copyright 2011 the Go authors)")
        .about("
cindex prepares the trigram index for use by csearch.  The index is the
file named by $CSEARCHINDEX, or else $HOME/.csearchindex.

The simplest invocation is

	cindex path...

which adds the file or directory tree named by each path to the index.
For example:

	cindex $HOME/src /usr/include

or, equivalently:

	cindex $HOME/src
	cindex /usr/include

If cindex is invoked with no paths, it reindexes the paths that have
already been added, in case the files have changed.  Thus, 'cindex' by
itself is a useful command to run in a nightly cron job.

By default cindex adds the named paths to the index but preserves
information about other paths that might already be indexed
(the ones printed by cindex -list).  The -reset flag causes cindex to
delete the existing index before indexing the new paths.
With no path arguments, cindex -reset removes the index.")
    .arg(clap::Arg::with_name("path")
         .index(1)
         .help("path to index"))
    .arg(clap::Arg::with_name("list-paths")
         .long("list")
         .help("list indexed paths and exit"))
    .arg(clap::Arg::with_name("reset-index")
         .long("reset")
         .conflicts_with("path").conflicts_with("list-paths")
         .help("discard existing index"))
    .arg(clap::Arg::with_name("INDEX_FILE")
         .long("indexpath")
         .takes_value(true)
         .help("use specified INDEX_FILE as the index path. overrides $CSEARCHINDEX"))
    .arg(clap::Arg::with_name("no-follow-simlinks")
         .long("no-follow-simlinks")
         .help("do not follow symlinked files and directories"))
    .arg(clap::Arg::with_name("MAX_FILE_SIZE_BYTES")
         .long("maxFileLen")
         .takes_value(true)
         .help("skip indexing a file if longer than this size in bytes"))
    .arg(clap::Arg::with_name("MAX_LINE_LEN_BYTES")
         .long("maxLineLen")
         .takes_value(true)
         .help("skip indexing a file if it has a line longer than this size in bytes"))
    .arg(clap::Arg::with_name("MAX_TRIGRAMS_COUNT")
         .long("maxtrigrams")
         .takes_value(true)
         .help("skip indexing a file if it has more than this number of trigrams"))
    .arg(clap::Arg::with_name("MAX_INVALID_UTF8_RATIO")
         .long("maxinvalidutf8ratio")
         .takes_value(true)
         .help("skip indexing a file if it has more than this ratio of invalid UTF-8 sequences"))
    .arg(clap::Arg::with_name("EXCLUDE_FILE")
         .long("exclude")
         .takes_value(true)
         .help("path to file containing a list of file patterns to exclude from indexing"))
    .arg(clap::Arg::with_name("FILE")
         .long("filelist")
         .takes_value(true)
         .help("path to file containing a list of file paths to index"))
    .arg(clap::Arg::with_name("verbose")
         .long("verbose")
         .help("print extra information"))
    .arg(clap::Arg::with_name("logskip")
         .long("logskip")
         .help("print why a file was skipped from indexing"))
    .get_matches();

    let max_log_level = if matches.is_present("verbose") {
        LogLevelFilter::Trace
    } else {
        LogLevelFilter::Info
    };
    customlogger::init(max_log_level).unwrap();

    let mut excludes: Vec<String> = vec![".csearchindex".to_string()];
    let mut args = Vec::<String>::new();

    if let Some(p) = matches.value_of("path") {
        args.push(p.to_string());
    }
    
    matches.value_of("INDEX_FILE").map(|p| {
        env::set_var("CSEARCHINDEX", p);
    });

    if matches.is_present("list-paths") {
        let i = open_index_or_fail();
        for each_file in i.indexed_paths() {
            println!("{}", each_file);
        }
        return;
    }
    if matches.is_present("reset-index") {
        let index_path = index::csearch_index();
        let p = Path::new(&index_path);
        if !p.exists() {
            // does not exist so nothing to do
            return;
        }
        let meta = p.metadata().expect("failed to get metadata for file!").file_type();
        if is_regular_file(meta) {
            std::fs::remove_file(p).expect("failed to remove file");
        }
    }
    if let Some(exc_path_str) = matches.value_of("EXCLUDE_FILE") {
        let exclude_path = Path::new(exc_path_str);
        let f = BufReader::new(File::open(exclude_path).expect("exclude file open error"));
        excludes.extend(f.lines().map(|f| f.unwrap().trim().to_string()));
    }
    if let Some(file_list_str) = matches.value_of("FILE") {
        let file_list = Path::new(file_list_str);
        let f = BufReader::new(File::open(file_list).expect("filelist file open error"));
        args.extend(f.lines().map(|f| f.unwrap().trim().to_string()));
    }

    if args.is_empty() {
        let i = open_index_or_fail();
        for each_file in i.indexed_paths() {
            args.push(each_file);
        }
    }

    let mut paths: Vec<PathBuf> = args.iter()
        .filter(|f| !f.is_empty())
        .map(|f| env::current_dir().unwrap().join(f).canonicalize().unwrap())
        .collect();
    paths.sort();

    let mut needs_merge = false;
    let mut index_path = index::csearch_index();
    if Path::new(&index_path).exists() {
        needs_merge = true;
        index_path.push('~');
    }

    let (tx, rx) = mpsc::channel::<OsString>();
    // copying these variables into the worker thread
    let index_path_cloned = index_path.clone();
    let paths_cloned = paths.clone();
    let log_skipped = matches.is_present("logskip");
    let h = thread::spawn(move || {
        let mut seen = HashSet::<OsString>::new();
        let mut i = IndexWriter::new(index_path_cloned);
        if let Some(t) = get_value_from_matches::<u64>(&matches, "MAX_TRIGRAMS_COUNT") {
            i.max_trigram_count = t;
        }
        if let Some(u) = get_value_from_matches::<f64>(&matches, "MAX_INVALID_UTF8_RATIO") {
            i.max_utf8_invalid = u;
        }
        if let Some(s) = get_value_from_matches::<u64>(&matches, "MAX_FILE_SIZE_BYTES") {
            i.max_file_len = s;
        }
        if let Some(b) = get_value_from_matches::<u64>(&matches, "MAX_LINE_LEN_BYTES") {
            i.max_line_len = b;
        }
        i.add_paths(paths_cloned.into_iter().map(PathBuf::into_os_string));
        let _frame = profiling::profile("Index files");
        while let Ok(f) = rx.recv() {
            if !seen.contains(&f) {
                match i.add_file(&f) {
                    Ok(_) => (),
                    Err(ref e) => {
                        match e.kind() {
                            IndexErrorKind::IoError(_) => panic!("{}: {}", Path::new(&f).display(), e),
                            _ => {
                                if log_skipped {
                                    warn!("{:?}: skipped. {}", f, e);
                                }
                                ()
                            }
                        }
                    }
                }
                seen.insert(f);
            }
        }
        info!("flush index");
        i.flush().expect("failed to flush index to disk");
        drop(_frame);
        profiling::print_profiling();
    });

    for p in paths {
        info!("index {}", p.display());
        let tx = tx.clone();
        walk_dir(Path::new(&p), &move |d: &DirEntry| {
            tx.send(d.path().into_os_string()).unwrap();
        }).unwrap();
    }
    drop(tx);
    h.join().unwrap();
    if needs_merge {
        let dest_path = index_path.clone() + &"~";
        let src1_path = index::csearch_index();
        let src2_path = index_path.clone();
        info!("merge {} {}", src1_path, src2_path);
        index::merge::merge(dest_path, src1_path, src2_path).unwrap();
        fs::remove_file(index_path.clone()).unwrap();
        fs::remove_file(index::csearch_index()).unwrap();
        fs::rename(index_path + &"~", index::csearch_index()).unwrap();
    }

    info!("done");
}

fn open_index_or_fail() -> IndexReader {
    let index_path = index::csearch_index();
    match IndexReader::open(&index_path) {
        Ok(i) => i,
        Err(e) => {
            error!("open {}: {}", index_path, e);
            std::process::exit(101);
        }
    }
}
