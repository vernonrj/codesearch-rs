use std::io::{self, BufReader, BufWriter, SeekFrom, BufRead, Read, Seek, Write};

pub use self::write::IndexWriter;
pub use self::error::{IndexResult, IndexError, IndexErrorKind};


mod write;
mod error;
mod sparseset;

mod postentry;
mod postheap;
mod trigramiter;

const NPOST: usize = (64 << 20) / 8;

pub fn get_offset<S: Seek>(seekable: &mut S) -> io::Result<u64> {
    seekable.seek(SeekFrom::Current(0))
}

pub fn copy_file<R: Read + Seek, W: Write>(dest: &mut BufWriter<W>, src: &mut R) {
    src.seek(SeekFrom::Start(0)).unwrap();
    let mut buf_src = BufReader::new(src); 
    loop {
        let length = if let Ok(b) = buf_src.fill_buf() {
            if b.len() == 0 {
                break;
            }
            dest.write_all(b).unwrap();
            b.len()
        } else {
            break;
        };
        buf_src.consume(length);
    }
}

