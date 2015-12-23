/// Copyright 2011 The Go Authors.  All rights reserved.
/// Use of this source code is governed by a BSD-style
/// license that can be found in the LICENSE file.
use regex::Regex;


#[derive(Debug)]
pub struct MatchOptions {
    pub pattern: Regex,
    pub print_count: bool,
    pub ignore_case: bool,
    pub files_with_matches_only: bool,
    pub line_number: bool,
    pub max_count: Option<usize>
}

