/// Implements reading from the index
use std::path::Path;
use std::io;
use std::u32;
use std::fmt;
use std::fmt::Debug;

use memmap::{Mmap, MmapView, Protection};
use std::io::Cursor;
use byteorder::{BigEndian, ReadBytesExt};
use varint::VarintRead;

use index::regexp::{Query, QueryOperation};
use index::search;

static TRAILER_MAGIC: &'static str = "\ncsearch trailr\n";
const POST_ENTRY_SIZE: usize = 3 + 4 + 4;


/**
 * Implementation of the index
 */
pub struct Index {
    data: Mmap,
    path_data: u32,
    name_data: u32,
    post_data: u32,
    name_index: usize,
    post_index: usize,
    num_name: usize,
    num_post: usize
}

impl Debug for Index {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "({}, {}, {}, {}, {}, {}, {}",
               self.path_data, self.name_data, self.post_data,
               self.name_index, self.post_index, self.num_name, self.num_post)
    }
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
                let name_index = extract_data_from_mmap(&m, n + 12) as usize;
                let post_index = extract_data_from_mmap(&m, n + 16) as usize;
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
    pub fn query(&self, query: Query, restrict: Option<Vec<u32>>) -> Option<Vec<u32>> {
        match query.operation {
            QueryOperation::None => Some(Vec::new()),
            QueryOperation::All => {
                if restrict.is_some() {
                    return restrict;
                }
                let mut v = Vec::<u32>::new();
                for idx in 0 .. self.num_name {
                    v.push(idx as u32);
                }
                Some(v)
            },
            QueryOperation::And => {
                let mut m_v = None;
                for trigram in query.trigram {
                    let bytes = trigram.as_bytes();
                    let tri_val = (bytes[0] as u32) << 16
                                | (bytes[1] as u32) << 8
                                | (bytes[2] as u32);
                    if m_v.is_none() {
                        m_v = Some(PostReader::list(&self, tri_val, &restrict));
                    } else {
                        m_v = Some(PostReader::and(&self, m_v.unwrap(), tri_val, &restrict));
                    }
                    assert!(m_v.is_some());
                    if let &Some(ref v) = &m_v {
                        if v.is_empty() {
                            return None;
                        }
                    }
                }
                for sub in query.sub {
                    // if m_v.is_none() {
                    //     m_v = restrict;
                    // }
                    m_v = self.query(sub, m_v);
                    match m_v {
                        None => return None,
                        Some(ref v) if v.len() == 0 => return None,
                        _ => ()
                    }
                }
                return m_v;
            },
            QueryOperation::Or => unimplemented!()
        }
    }
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
    pub fn name(&self, file_id: usize) -> String {
        let offset = self.extract_data(self.name_index + 4 * file_id);
        self.extract_string_at((self.name_data + offset) as usize)
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
    fn find_list(&self, trigram: u32) -> (isize, u32) {
        let d: &[u8] = unsafe {
            let s = self.data.as_slice();
            let (_, right_side) = s.split_at(self.post_index);
            let (d, _) = right_side.split_at(POST_ENTRY_SIZE * self.num_post);
            d
        };
        println!("size of slice = {}", d.len());
        let result = search::search(self.num_post, |i| {
            let i_scaled = i * POST_ENTRY_SIZE;
            let tri_val = (d[i_scaled] as u32) << 16
                        | (d[i_scaled+1] as u32) << 8
                        | (d[i_scaled+2] as u32);
            tri_val >= trigram
        });
        if result >= self.num_post {
            return (0, 0);
        }
        let result_scaled: usize = result * POST_ENTRY_SIZE;
        let tri_val = (d[result_scaled] as u32) << 16
                    | (d[result_scaled+1] as u32) << 8
                    | (d[result_scaled+2] as u32);
        if tri_val != trigram {
            return (0, 0);
        }
        let count = {
            let (_, mut right) = d.split_at(result_scaled+3);
            right.read_i32::<BigEndian>().unwrap() as isize
        };
        let offset = {
            let (_, mut right) = d.split_at(result_scaled+3+4);
            right.read_u32::<BigEndian>().unwrap()
        };
        (count, offset)
    }
}

#[derive(Debug)]
struct PostReader<'a, 'b> {
    index: &'a Index,
    count: isize,
    offset: u32,
    fileid: i64,
    d: Cursor<Vec<u8>>,
    restrict: &'b Option<Vec<u32>>
}

impl<'a, 'b> PostReader<'a, 'b> {
    fn new(index: &'a Index, trigram: u32, restrict: &'b Option<Vec<u32>>) -> Option<Self> {
        let (count, offset) = index.find_list(trigram);
        println!("{}, {}", count, offset);
        if count == 0 {
            return None;
        }
        let view = unsafe {
            let v = index.data.as_slice();
            let split_point = (index.post_data as usize) + (offset as usize) + 3;
            let (_, v1) = v.split_at(split_point);
            Cursor::new(v1.iter().cloned().collect::<Vec<_>>())
        };
        Some(PostReader {
            index: index,
            count: count,
            offset: offset,
            fileid: -1,
            d: view,
            restrict: restrict
        })
    }
    pub fn and(index: &'a Index,
               list: Vec<u32>,
               trigram: u32,
               restrict: &'b Option<Vec<u32>>) -> Vec<u32>
    {
        if let Some(mut r) = Self::new(index, trigram, restrict) {
            let mut v = Vec::new();
            while r.next() {
                let fileid = r.fileid;
                let mut i = 0;
                while i < list.len() && (list[i] as i64) < fileid {
                    i += 1;
                }
                if i < list.len() && (list[i] as i64) == fileid {
                    assert!(fileid >= 0);
                    v.push(fileid as u32);
                    i += 1;
                }
            }
            v
        } else {
            Vec::new()
        }
    }
    pub fn list(index: &'a Index, trigram: u32, restrict: &Option<Vec<u32>>) -> Vec<u32> {
        if let Some(mut r) = Self::new(index, trigram, restrict) {
            let mut x = Vec::<u32>::new();
            while r.next() {
                x.push(r.fileid as u32);
            }
            x
        } else {
            Vec::new()
        }
    }
    // TODO: refactor either to use rust iterator or don't look like an iterator
    fn next(&mut self) -> bool {
        while self.count > 0 {
            self.count -= 1;
            let delta = self.d.read_unsigned_varint_32().unwrap();
            self.fileid += delta as i64;
            // TODO: add .restrict
            return true;
        }
        // list should end with terminating 0 delta
        // TODO: add bounds checking
        self.fileid = -1;
        return false;
    }
}
