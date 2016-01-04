// Copyright 2015 Vernon Jones.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

use regex::Regex;
use std::io::{self, BufRead, BufReader, Write};
use std::fs::File;
use std::path::Path;

/**
 * Search files for matching expressions
 *
 * Grep takes a pattern an applies it to files, returning
 * an iterator for walking over matches in a file.
 *
 * ```no_run
 * # extern crate codesearch_lib;
 * # extern crate regex;
 * # use regex::Regex;
 * # use codesearch_lib::grep::grep::Grep;
 * # use codesearch_lib::grep::grep::GrepIter;
 * # use std::io;
 * # fn main() { foo(); }
 * # fn foo() -> io::Result<()> {
 * let g = Grep::new(Regex::new(r"Pattern").unwrap());
 *
 * let it: GrepIter = try!(g.open("foo.txt"));
 *
 * for each_result in it {
 *     println!("match = {}", each_result.line);
 * }
 * # Ok(())
 * # }
 * ```
 *
 */
pub struct Grep {
    expression: Regex
}

impl Grep {
    /// Takes a regular expression and returns a Grep instance
    ///
    /// ```rust
    /// # extern crate regex;
    /// # extern crate codesearch_lib;
    /// # use codesearch_lib::grep::grep::Grep;
    /// use regex::Regex;
    /// # fn main() {
    /// let g = Grep::new(Regex::new(r"Pattern").unwrap());
    /// # }
    /// ```
    pub fn new(expression: Regex) -> Self {
        Grep {
            expression: expression
        }
    }

    /// Takes a filename and returns a GrepIter. Fails if the file open fails.
    ///
    /// ```no_run
    /// # extern crate codesearch_lib;
    /// # extern crate regex;
    /// use std::io;
    /// # use regex::Regex;
    /// # use codesearch_lib::grep::grep::{Grep, GrepIter};
    /// # fn main() { foo(); }
    /// # fn foo() -> io::Result<()> {
    /// let g = Grep::new(Regex::new(r"Pattern").unwrap());
    /// let it: GrepIter = try!(g.open("foo.txt"));
    /// # Ok(())
    /// # }
    /// ```
    pub fn open<P: AsRef<Path> + ToString>(&self, path: P) -> io::Result<GrepIter> {
        File::open(path).map(|f| {
            GrepIter {
                expression: self.expression.clone(),
                open_file: Box::new(BufReader::new(f).lines().enumerate())
            }
        })
    }
}

/**
 * Iterator over a file, returning matches to an expression.
 *
 * Returned from Grep::open
 */
pub struct GrepIter {
    expression: Regex,
    open_file: Box<Iterator<Item=(usize, io::Result<String>)>>
}

impl GrepIter {
    fn filter_line(&self, l: &String) -> bool {
        self.expression.is_match(&l)
    }
}

/**
 * Match for an expression in a file.
 *
 * Returned from GrepIter::next().
 */
#[derive(Debug)]
pub struct MatchResult {
    /// The line that matched
    pub line: String,

    /// The line number of the line that matched
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
