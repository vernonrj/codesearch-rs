// Copyright 2015 Vernon Jones.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

use regex::Regex;
use std::io::{self, BufRead, BufReader};
use std::fs::File;
use std::path::Path;

/**
 * Search files for matching expressions
 *
 * Grep takes a pattern an applies it to files, returning
 * an iterator for walking over matches in a file.
 *
 * ```no_run
 * # extern crate regex;
 * # extern crate libcsearch;
 * # use regex::Regex;
 * # use libcsearch::grep::{Grep, GrepIter};
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
    expression: Regex,
}

impl Grep {
    /// Takes a regular expression and returns a Grep instance
    ///
    /// ```rust
    /// # extern crate regex;
    /// # extern crate libcsearch;
    /// # use libcsearch::grep::Grep;
    /// use regex::Regex;
    /// # fn main() {
    /// let g = Grep::new(Regex::new(r"Pattern").unwrap());
    /// # }
    /// ```
    pub fn new(expression: Regex) -> Self {
        Grep { expression: expression }
    }

    /// Takes a filename and returns a GrepIter. Fails if the file open fails.
    ///
    /// ```no_run
    /// # extern crate regex;
    /// # extern crate libcsearch;
    /// use std::io;
    /// # use regex::Regex;
    /// # use libcsearch::grep::{Grep, GrepIter};
    /// # fn main() { foo(); }
    /// # fn foo() -> io::Result<()> {
    /// let g = Grep::new(Regex::new(r"Pattern").unwrap());
    /// let it: GrepIter = try!(g.open("foo.txt"));
    /// # Ok(())
    /// # }
    /// ```
    pub fn open<P: AsRef<Path>>(&self, path: P) -> io::Result<GrepIter> {
        File::open(path).map(|f| {
            GrepIter {
                expression: self.expression.clone(),
                open_file: BufReader::new(f),
                line_number: 0,
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
    open_file: BufReader<File>,
    line_number: usize,
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
    pub line_number: usize,
}

const NEWLINE_BYTE: u8 = 0x0a;

impl Iterator for GrepIter {
    type Item = MatchResult;
    fn next(&mut self) -> Option<Self::Item> {
        let mut raw_line = Vec::new();
        while let Ok(n) = self.open_file.read_until(NEWLINE_BYTE, &mut raw_line) {
            if n == 0 {
                break;
            }
            // remove newline
            raw_line.pop();
            self.line_number += 1;
            {
                let line = String::from_utf8_lossy(&raw_line);
                if self.expression.is_match(&line) {
                    return Some(MatchResult {
                        line: line.into_owned(),
                        line_number: self.line_number - 1,
                    });
                }
            }
            raw_line.clear();
        }
        // done with file
        None
    }
}
