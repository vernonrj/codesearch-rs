#![allow(dead_code)]
extern crate tempfile;

extern crate libcindex;
extern crate libcsearch;

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};


use self::libcindex::writer::IndexWriter;

pub fn build_index<P: AsRef<Path>>(out: P,
                                   paths: Vec<PathBuf>,
                                   file_data: BTreeMap<&'static str, &'static str>) {
    build_flush_index(out, paths, false, file_data);
}

pub fn build_flush_index<P: AsRef<Path>>(out: P,
                                         paths: Vec<PathBuf>,
                                         do_flush: bool,
                                         file_data: BTreeMap<&'static str, &'static str>) {
    let mut ix = IndexWriter::new(out.as_ref()).unwrap();
    ix.add_paths(paths.into_iter().map(PathBuf::into_os_string));
    let mut files = file_data.keys().collect::<Vec<_>>();
    files.sort();
    for name in files {
        let r = file_data[name];
        let len = r.len() as u64;
        ix.add(name, Cursor::new(r.as_bytes()), len).unwrap();
    }
    if do_flush {
        ix.flush_post().unwrap();
    }
    ix.flush().unwrap();
}

pub fn tri<T: Into<char>>(x: T, y: T, z: T) -> u32 {
    ((x.into() as u32) << 16) | ((y.into() as u32) << 8) | (z.into() as u32)
}
