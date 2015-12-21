/// Implements reading from the index
use std::path::Path;
use std::io;

use memmap::{Mmap, Protection};
use std::io::Cursor;
use byteorder::{BigEndian, ReadBytesExt};

// use index::regexp::{Query, QueryOperation};

static TRAILER_MAGIC: &'static str = "\ncsearch trailr\n";


/**
 * Implementation of the index
 */
pub struct Index {
    data: Mmap,
    path_data: u32,
    name_data: u32,
    post_data: u32,
    name_index: u32,
    post_index: u32,
    num_name: usize,
    num_post: usize
}

fn extract_data_from_mmap(data: &Mmap, offset: usize) -> u32 {
    unsafe {
        let mut buf = Cursor::new(&data.as_slice()[ offset .. offset + 4]);
        buf.read_u32::<BigEndian>().unwrap()
    }
}


impl Index {
    fn extract_data(&self, offset: usize) -> u32 {
        unsafe {
            let mut buf = Cursor::new(&self.data.as_slice()[ offset .. offset + 4]);
            buf.read_u32::<BigEndian>().unwrap()
        }
    }
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Index> {
        Mmap::open_path(path, Protection::Read)
            .map(|m| {
                let n = m.len() - (TRAILER_MAGIC.bytes().len()) - 5*4;
                let path_data = extract_data_from_mmap(&m, n);
                let name_data = extract_data_from_mmap(&m, n + 4);
                let post_data = extract_data_from_mmap(&m, n + 8);
                let name_index = extract_data_from_mmap(&m, n + 12);
                let post_index = extract_data_from_mmap(&m, n + 16);
                let num_name: usize = if post_index > name_index {
                    let d = (post_index - name_index) / 4;
                    if d == 0 { 0 } else { (d - 1) as usize }
                } else {
                    0
                };
                let num_post = if n > (post_index as usize) {
                    (n - (post_index as usize)) / (3 + 4 + 4)
                } else {
                    0
                };
                Index {
                    data: m,
                    path_data: path_data,
                    name_data: name_data,
                    post_data: post_data,
                    name_index: name_index,
                    post_index: post_index,
                    num_name: num_name,
                    num_post: num_post
                }
        })
    }
    // pub fn query(&self, query: Query) -> Vec<u32> {
    //     let v = Vec::new();
    //     match query.operation {
    //         QueryOperation::None => Vec::new(),
    //         QueryOperation::All => {
    //             let mut v = Vec::new();
    //             for idx in 0 .. self.num_name {
    //                 v.push(self.extract_data(idx));
    //             }
    //             v
    //         },
    //         QueryOperation::And => {
    //             for trigram in query.trigram {
    //                 let tri_val = self.extract_data(trigram[0]) << 16
    //                             | self.extract_data(trigram[1]) << 8
    //                             | self.extract_data(trigram[2]);
    //             }
    //         }
    //     }
    // }
    pub fn len(&self) -> usize {
        self.data.len()
    }
    pub unsafe fn as_slice(&self) -> &[u8] {
        self.data.as_slice()
    }
    pub fn indexed_paths(&self) -> Vec<String> {
        let mut paths = Vec::new();
        let mut offset = self.path_data as usize;
        loop {
            let s = self.extract_string_at(offset);
            if s.len() == 0 {
                break;
            }
            offset += s.len() + 1;
            paths.push(s);
        }
        paths
    }
    fn extract_string_at(&self, offset: usize) -> String {
        unsafe {
            let mut index = 0;
            let mut s = String::new();
            let sl = self.as_slice();
            while sl[offset + index] != 0 {
                s.push(sl[offset+index] as char);
                index += 1;
            }
            s
        }
    }
}
