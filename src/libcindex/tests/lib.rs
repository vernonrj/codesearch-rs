extern crate libcindex;
extern crate tempfile;

use std::collections::BTreeMap;
use std::io::{Cursor, Read};
use std::ops::DerefMut;
use std::num::Wrapping;
use std::path::Path;
use std::u32;

use tempfile::NamedTempFile;

use libcindex::writer::IndexWriter;


fn trivial_files() -> BTreeMap<&'static str, &'static str> {
    let mut d = BTreeMap::new();
    d.insert("f0",       "\n\n");
    d.insert("file1",    "\na\n");
    d.insert("thefile2", "\nab\n");
    d.insert("file3",    "\nabc\n");
    d.insert("afile4",   "\ndabc\n");
    d.insert("file5",    "\nxyzw\n");
    d
}

fn trivial_index() -> Vec<u8> {
    let mut s = Vec::<u8>::new();
    // header
    s.extend_from_slice("csearch index 1\n".as_bytes());

    // list of paths
    s.extend_from_slice("\x00".as_bytes());

    // list of names
    s.extend_from_slice("afile4\x00".as_bytes());
    s.extend_from_slice("f0\x00".as_bytes());
    s.extend_from_slice("file1\x00".as_bytes());
    s.extend_from_slice("file3\x00".as_bytes());
    s.extend_from_slice("file5\x00".as_bytes());
    s.extend_from_slice("thefile2\x00".as_bytes());
    s.extend_from_slice("\x00".as_bytes());

    // list of posting lists
    s.extend("\na\n".as_bytes());         s.extend_from_slice(&mut file_list(vec![2])); // file1
    s.extend("\nab".as_bytes());          s.extend_from_slice(&mut file_list(vec![3, 5])); // file3, thefile2
    s.extend("\nda".as_bytes());          s.extend_from_slice(&mut file_list(vec![0])); // afile4
    s.extend("\nxy".as_bytes());          s.extend_from_slice(&mut file_list(vec![4])); // file5
    s.extend("ab\n".as_bytes());          s.extend_from_slice(&mut file_list(vec![5])); // thefile2
    s.extend("abc".as_bytes());           s.extend_from_slice(&mut file_list(vec![0, 3])); // afile4, file3
    s.extend("bc\n".as_bytes());          s.extend_from_slice(&mut file_list(vec![0, 3])); // afile4, file3
    s.extend("dab".as_bytes());           s.extend_from_slice(&mut file_list(vec![0])); // afile4
    s.extend("xyz".as_bytes());           s.extend_from_slice(&mut file_list(vec![4])); // file5
    s.extend("yzw".as_bytes());           s.extend_from_slice(&mut file_list(vec![4])); // file5
    s.extend("zw\n".as_bytes());          s.extend_from_slice(&mut file_list(vec![4])); // file5
    s.push(0xff); s.push(0xff); s.push(0xff);
    s.extend_from_slice(&mut file_list(vec![]));

    // name index
    s.extend_from_slice(&mut u32_to_vec(0));
    s.extend_from_slice(&mut u32_to_vec(6+1));
    s.extend_from_slice(&mut u32_to_vec(6+1+2+1));
    s.extend_from_slice(&mut u32_to_vec(6+1+2+1+5+1));
    s.extend_from_slice(&mut u32_to_vec(6+1+2+1+5+1+5+1));
    s.extend_from_slice(&mut u32_to_vec(6+1+2+1+5+1+5+1+5+1));
    s.extend_from_slice(&mut u32_to_vec(6+1+2+1+5+1+5+1+5+1+8+1));

    // posting list index,
    s.extend("\na\n".as_bytes());        s.extend_from_slice(&mut u32_to_vec(1)); s.extend_from_slice(&mut u32_to_vec(0));
    s.extend("\nab".as_bytes());         s.extend_from_slice(&mut u32_to_vec(2)); s.extend_from_slice(&mut u32_to_vec(5));
    s.extend("\nda".as_bytes());         s.extend_from_slice(&mut u32_to_vec(1)); s.extend_from_slice(&mut u32_to_vec(5+6));
    s.extend("\nxy".as_bytes());         s.extend_from_slice(&mut u32_to_vec(1)); s.extend_from_slice(&mut u32_to_vec(5+6+5));
    s.extend("ab\n".as_bytes());         s.extend_from_slice(&mut u32_to_vec(1)); s.extend_from_slice(&mut u32_to_vec(5+6+5+5));
    s.extend("abc".as_bytes());          s.extend_from_slice(&mut u32_to_vec(2)); s.extend_from_slice(&mut u32_to_vec(5+6+5+5+5));
    s.extend("bc\n".as_bytes());         s.extend_from_slice(&mut u32_to_vec(2)); s.extend_from_slice(&mut u32_to_vec(5+6+5+5+5+6));
    s.extend("dab".as_bytes());          s.extend_from_slice(&mut u32_to_vec(1)); s.extend_from_slice(&mut u32_to_vec(5+6+5+5+5+6+6));
    s.extend("xyz".as_bytes());          s.extend_from_slice(&mut u32_to_vec(1)); s.extend_from_slice(&mut u32_to_vec(5+6+5+5+5+6+6+5));
    s.extend("yzw".as_bytes());          s.extend_from_slice(&mut u32_to_vec(1)); s.extend_from_slice(&mut u32_to_vec(5+6+5+5+5+6+6+5+5));
    s.extend("zw\n".as_bytes());         s.extend_from_slice(&mut u32_to_vec(1)); s.extend_from_slice(&mut u32_to_vec(5+6+5+5+5+6+6+5+5+5));
    s.push(0xff); s.push(0xff); s.push(0xff);
    s.extend(u32_to_vec(0)); s.extend_from_slice(&mut u32_to_vec(5+6+5+5+5+6+6+5+5+5+5));

    // trailer
    s.extend_from_slice(&mut u32_to_vec(16));
    s.extend_from_slice(&mut u32_to_vec(16+1));
    s.extend_from_slice(&mut u32_to_vec(16+1+38));
    s.extend_from_slice(&mut u32_to_vec(16+1+38+62));
    s.extend_from_slice(&mut u32_to_vec(16+1+38+62+28));

    s.extend_from_slice("\ncsearch trailr\n".as_bytes());

    s
}

fn file_list(list: Vec<u32>) -> Vec<u8> {
    let mut buf = Vec::<u8>::new();
    let mut last = u32::MAX;

    for x in list {
        let Wrapping(mut delta) = Wrapping(x) - Wrapping(last);
        while delta >= 0x80 {
            buf.push((delta | 0x80) as u8);
            delta >>= 7;
        }
        buf.push(delta as u8);
        last = x;
    }
    buf.push(0);
    buf
}

fn u32_to_vec(value: u32) -> Vec<u8> {
    let mut v = Vec::new();
    v.push((value >> 24) as u8);
    v.push(((value >> 16) & 0xff) as u8);
    v.push(((value >> 8) & 0xff) as u8);
    v.push((value & 0xff) as u8);
    v
}

#[test]
fn test_trivial_write() {
    test_write(false);
}

#[test]
fn test_trivial_write_disk() {
    test_write(true);
}

fn test_write(do_flush: bool) {
    let mut f = NamedTempFile::new().unwrap();
    {
        let out = f.path();
        build_flush_index(out, do_flush, trivial_files());
    }

    let mut data = Vec::new();
    f.deref_mut().read_to_end(&mut data).unwrap();
    let want = trivial_index();
    if data != want {
        let mut i = 0;
        while i < data.len() && i < want.len() && data[i] == want[i] {
            i += 1;
        }
        panic!("wrong index:\nhave {:?} {:?}\nwant {:?} {:?}\ncommon bytes: {}",
               &data[..i], &data[i..], &want[..i], &want[i..], i);
    }
}

fn build_flush_index<'a>(out: &'a Path,
                         do_flush: bool,
                         file_data: BTreeMap<&'static str, &'static str>)
{
    let mut ix = IndexWriter::new(out).unwrap();
    ix.add_paths(vec![]);
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
