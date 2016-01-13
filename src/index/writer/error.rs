use std::error::{self, Error};
use std::fmt;
use std::io;

use index::byteorder;

#[derive(Debug)]
pub struct IndexError {
    kind: IndexErrorKind,
    error: Box<error::Error + Send + Sync>
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexErrorKind {
    IoError(io::ErrorKind),
    ByteorderError,
    FileNameError,
    FileTooLong,
    LineTooLong,
    TooManyTrigrams,
    BinaryDataPresent,
    HighInvalidUtf8Ratio
}


impl IndexError {
    pub fn new<E>(kind: IndexErrorKind, error: E) -> IndexError
        where E: Into<Box<error::Error + Send + Sync>>
    {
        IndexError {
            kind: kind,
            error: error.into()
        }
    }
    pub fn kind(&self) -> IndexErrorKind {
        self.kind.clone()
    }
}

impl From<io::Error> for IndexError {
    fn from(e: io::Error) -> Self {
        IndexError {
            kind: IndexErrorKind::IoError(e.kind()),
            error: Box::new(e)
        }
    }
}

impl From<byteorder::Error> for IndexError {
    fn from(e: byteorder::Error) -> Self {
        match e {
            byteorder::Error::Io(err) => IndexError::from(err),
            err @ _ => IndexError {
                kind: IndexErrorKind::ByteorderError,
                error: Box::new(err)
            }
        }
    }
}

impl From<IndexError> for io::Error {
    fn from(e: IndexError) -> Self {
        match e.kind() {
            IndexErrorKind::IoError(ekind) => {
                io::Error::new(ekind, e)
            },
            _ => io::Error::new(io::ErrorKind::Other, e)
        }
    }
}

impl Error for IndexError {
    fn description(&self) -> &str {
        match self.kind {
            IndexErrorKind::IoError(_) => self.error.description(),
            IndexErrorKind::ByteorderError => self.error.description(),
            IndexErrorKind::FileNameError => "filename conversion error",
            IndexErrorKind::FileTooLong => "file too long",
            IndexErrorKind::LineTooLong => "line too long",
            IndexErrorKind::TooManyTrigrams => "too many trigrams in file",
            IndexErrorKind::BinaryDataPresent => "binary file",
            IndexErrorKind::HighInvalidUtf8Ratio => "Too many invalid utf-8 sequences"
        }
    }
}

impl fmt::Display for IndexError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        self.error.fmt(fmt)
    }
}


pub type IndexResult<T> = Result<T, IndexError>;

