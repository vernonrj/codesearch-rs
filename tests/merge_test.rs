extern crate tempfile;

extern crate libcindex;
extern crate libcsearch;

mod common;

use std::collections::BTreeMap;
use std::path::PathBuf;

use self::tempfile::NamedTempFile;

use self::libcindex::merge::merge;
use self::libcsearch::reader::{PostReader, IndexReader};

use common::{build_index, tri};

const MERGE_PATHS_1: [&'static str; 3] = [
    "/a",
    "/b",
    "/c"
];

const MERGE_PATHS_2: [&'static str; 2] = [
    "/b",
    "/cc"
];

fn merge_files_1() -> BTreeMap<&'static str, &'static str> {
    let mut m = BTreeMap::new();
    m.insert("/a/x",  "hello world");
    m.insert("/a/y",  "goodbye world");
    m.insert("/b/xx", "now is the time");
    m.insert("/b/xy", "for all good men");
    m.insert("/c/ab", "give me all the potatoes");
    m.insert("/c/de", "or give me death now");
    m
}

fn merge_files_2() -> BTreeMap<&'static str, &'static str> {
    let mut m = BTreeMap::new();
    m.insert("/b/www", "world wide indeed");
    m.insert("/b/xx",  "no, not now");
    m.insert("/b/yy",  "first potatoes, now liberty?");
    m.insert("/cc",    "come to the aid of his potatoes");
    m
}

#[test]
fn test_merge() {
    let f1 = NamedTempFile::new().unwrap();
    build_index(f1.path(), MERGE_PATHS_1.iter().map(PathBuf::from).collect(), merge_files_1());
    let f2 = NamedTempFile::new().unwrap();
    build_index(f2.path(), MERGE_PATHS_2.iter().map(PathBuf::from).collect(), merge_files_2());
    let f3 = NamedTempFile::new().unwrap();

    merge(f3.path(), f1.path(), f2.path()).unwrap();

    let ix1 = IndexReader::open(f1.path()).unwrap();
    let ix2 = IndexReader::open(f2.path()).unwrap();
    let ix3 = IndexReader::open(f3.path()).unwrap();

    fn check_files(ix: &IndexReader, l: &[&'static str]) {
        for (i, fname) in l.iter().enumerate() {
            assert_eq!(&ix.name(i as u32), fname);
        }
    }

    check_files(&ix1, &["/a/x", "/a/y", "/b/xx", "/b/xy", "/c/ab", "/c/de"]);
    check_files(&ix2, &["/b/www", "/b/xx", "/b/yy", "/cc"]);
    check_files(&ix3, &["/a/x", "/a/y", "/b/www", "/b/xx", "/b/yy", "/c/ab", "/c/de", "/cc"]);

    fn check(ix: &IndexReader, trig: &str, l: &[u32]) {
        let t = trig.chars().collect::<Vec<char>>();
        let l1 = PostReader::list(ix, tri(t[0], t[1], t[2]), &mut None);
        assert_eq!(l1, l);
    }

    check(&ix1, "wor", &[0, 1]);
    check(&ix1, "now", &[2, 5]);
    check(&ix1, "all", &[3, 4]);

    check(&ix2, "now", &[1, 2]);

    check(&ix3, "all", &[5]);
    check(&ix3, "wor", &[0, 1, 2]);
    check(&ix3, "now", &[3, 4, 6]);
    check(&ix3, "pot", &[4, 5, 7]);
}
