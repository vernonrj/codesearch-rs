/// Search files for matches

use regex::Regex;
use std::io::{self, BufRead, BufReader};
use std::fs::File;
use std::path::Path;

use csearch_regex::matcher::MatchOptions;

pub struct Grep {
    expression: Regex,
    pub options: GrepOptions
}

#[derive(Default, Clone)]
pub struct GrepOptions {
    file_names_only: bool,
    print_count: bool,
    match_count: usize,
    print_line_numbers: bool
}

impl GrepOptions {
    pub fn from_match_options(options: &MatchOptions) -> Self {
        GrepOptions {
            file_names_only: options.files_with_matches_only,
            print_count: options.print_count,
            match_count: 0,
            print_line_numbers: options.line_number
        }
    }
}

impl Grep {
    pub fn new(expression: Regex, options: &MatchOptions) -> Self {
        Grep {
            expression: expression,
            options: GrepOptions::from_match_options(options)
        }
    }
    pub fn open<P: AsRef<Path> + ToString>(&self, path: P) -> io::Result<GrepIter> {
        let path_as_string = path.to_string();
        File::open(path).map(|f| {
            GrepIter {
                expression: self.expression.clone(),
                open_file: Box::new(BufReader::new(f).lines().enumerate()),
                filename: path_as_string,
                options: self.options.clone()
            }
        })
    }
}


pub struct GrepIter {
    expression: Regex,
    open_file: Box<Iterator<Item=(usize, io::Result<String>)>>,
    filename: String,
    options: GrepOptions
}

impl GrepIter {
    fn filter_line(&self, l: &String) -> bool {
        self.expression.is_match(&l)
    }
    fn all_lines_printed(&self) -> bool {
        if self.options.print_count || self.options.file_names_only {
            false
        } else {
            true
        }
    }
    fn only_filenames_printed(&self) -> bool {
        self.options.file_names_only
    }
    fn format_line(&mut self, line_number: usize, l: &String) -> Option<String> {
        self.options.match_count += 1;
        if self.all_lines_printed() {
            let mut out_line = String::new();
            out_line.push_str(&self.filename);
            out_line.push_str(":");
            if self.options.print_line_numbers {
                out_line.push_str(&(line_number + 1).to_string()); // 0-based to 1-based
                out_line.push_str(":");
            }
            out_line.push_str(l);
            Some(out_line)
        } else if self.only_filenames_printed() {
            unimplemented!();
        } else {
            None
        }
    }
}


impl Iterator for GrepIter {
    type Item = io::Result<String>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let m_l: Option<(usize, String)> = self.open_file.next()
                .and_then(|(line_number, line)| {
                    line.ok().map(|l| (line_number, l))
                }); // convert Result to Option
            if let Some((line_number, line)) = m_l {
                if self.filter_line(&line) {
                    return self.format_line(line_number, &line).map(|l| Ok(l));
                }
            } else {
                return None;
            }
        }
    }
}
