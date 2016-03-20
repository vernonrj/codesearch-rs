extern crate tempfile;

extern crate libcsearch;

mod common;

use std::collections::BTreeMap;

use self::tempfile::NamedTempFile;
use self::libcsearch::reader::{PostReader, IndexReader};

use common::{tri, build_index};

fn post_files() -> BTreeMap<&'static str, &'static str> {
    let mut m = BTreeMap::new();
    m.insert("file0", "");
    m.insert("file1", "Google Code Search");
    m.insert("file2", "Google Code Project Hosting");
    m.insert("file3", "Google Web Search");
    m
}


fn make_index() -> IndexReader {
    let f = NamedTempFile::new().unwrap();
    let out = f.path();
    build_index(out, vec![], post_files());
    IndexReader::open(out).unwrap()
}

#[test]
fn test_postreader_list() {
    let ix = make_index();
    assert_eq!(PostReader::list(&ix, tri('S', 'e', 'a'), &mut None),
               vec![1, 3]);
    assert_eq!(PostReader::list(&ix, tri('G', 'o', 'o'), &mut None),
               vec![1, 2, 3]);
}

#[test]
fn test_postreader_and() {
    let ix = make_index();
    assert_eq!(PostReader::and(&ix,
                               PostReader::list(&ix, tri('S', 'e', 'a'), &mut None),
                               tri('G', 'o', 'o'),
                               &mut None),
               vec![1, 3]);
    assert_eq!(PostReader::and(&ix,
                               PostReader::list(&ix, tri('G', 'o', 'o'), &mut None),
                               tri('S', 'e', 'a'),
                               &mut None),
               vec![1, 3]);
}

#[test]
fn test_postreader_or() {
    let ix = make_index();
    assert_eq!(PostReader::or(&ix,
                              PostReader::list(&ix, tri('G', 'o', 'o'), &mut None),
                              tri('S', 'e', 'a'),
                              &mut None),
               vec![1, 2, 3]);
    assert_eq!(PostReader::or(&ix,
                              PostReader::list(&ix, tri('S', 'e', 'a'), &mut None),
                              tri('G', 'o', 'o'),
                              &mut None),
               vec![1, 2, 3]);
}
