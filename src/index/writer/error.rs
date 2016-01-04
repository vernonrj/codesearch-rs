use std::error;
use std::fmt;
use std::io::{self, Error};

#[derive(Debug)]
pub struct IndexError {
    kind: IndexErrorKind,
    error: Box<error::Error + Send + Sync>
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexErrorKind {
    IoError(io::ErrorKind),
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

impl error::Error for IndexError {
    fn description(&self) -> &str {
        match self.kind {
            IndexErrorKind::IoError(_) => self.error.description(),
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

