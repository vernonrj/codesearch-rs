// Copyright 2015 Vernon Jones.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

use regex::Regex;
use std::io::{self, BufRead, BufReader, Write};
use std::fs::File;
use std::path::Path;

pub struct Grep {
    expression: Regex
}

impl Grep {
    pub fn new(expression: Regex) -> Self {
        Grep {
            expression: expression
        }
    }
    pub fn open<P: AsRef<Path> + ToString>(&self, path: P) -> io::Result<GrepIter> {
        File::open(path).map(|f| {
            GrepIter {
                expression: self.expression.clone(),
                open_file: Box::new(BufReader::new(f).lines().enumerate())
            }
        })
    }
}


pub struct GrepIter {
    expression: Regex,
    open_file: Box<Iterator<Item=(usize, io::Result<String>)>>
}

impl GrepIter {
    fn filter_line(&self, l: &String) -> bool {
        self.expression.is_match(&l)
    }
}

#[derive(Debug)]
pub struct MatchResult {
    pub line: String,
    pub line_number: usize
}

impl Iterator for GrepIter {
    type Item = MatchResult;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let maybe_next_line = self.open_file.next();
            if maybe_next_line.is_none() {
                return None;
            }
            let (line_number, next_line_result) = maybe_next_line.unwrap();
            if let Err(cause) = next_line_result {
                writeln!(&mut io::stderr(), "failed to read line: {}", cause).unwrap();
                return None;
            }
            let line = next_line_result.unwrap();
            if self.filter_line(&line) {
                return Some(MatchResult {
                    line: line.clone(),
                    line_number: line_number
                });
            }
        }
    }
}
