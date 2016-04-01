// Copyright 2015 Vernon Jones.
// Original code Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate regex;
extern crate regex_syntax;

extern crate consts;
extern crate libcustomlogger;
extern crate libcsearch;
extern crate libvarint;

use libcsearch::grep;
use libcsearch::reader::IndexReader;
use libcsearch::regexp::{RegexInfo, Query};

use std::io::{self, Write};
use std::collections::{HashMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};



#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum PrintFormat {
    Normal,
    VisualStudio,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum LinePart {
    Path,
    LineNumber,
    Separator,
    Match,
}

#[derive(Debug)]
pub struct MatchOptions {
    pub pattern: regex::Regex,
    pub print_format: PrintFormat,
    pub print_count: bool,
    pub ignore_case: bool,
    pub files_with_matches_only: bool,
    pub line_number: bool,
    pub with_color: bool,
    pub max_count: Option<usize>,
}

const ABOUT: &'static str = "
Csearch behaves like grep over all indexed files, searching for regexp,
an RE2 (nearly PCRE) regular expression.

Csearch relies on the existence of an up-to-date index created ahead of time.
To build or rebuild the index that csearch uses, run:

	cindex path...

where path... is a list of directories or individual files to be included in the index.
If no index exists, this command creates one.  If an index already exists, cindex
overwrites it.  Run cindex --help for more.

Csearch uses the index stored in $CSEARCHINDEX or, if that variable is unset or
empty, $HOME/.csearchindex.
";


#[cfg(feature = "color")]
mod color {
    extern crate libc;
    extern crate ansi_term;
    use self::ansi_term::Colour;
    use super::LinePart;

    use std::env;

    #[cfg(windows)]
    const STDOUT_FILENO: i32 = 1;
    #[cfg(not(windows))]
    const STDOUT_FILENO: i32 = libc::STDOUT_FILENO as i32;

    pub fn is_color_output_available() -> bool {
        let isatty = unsafe { libc::isatty(STDOUT_FILENO) != 0 };
        if !isatty {
            return false;
        }
        let t = if let Ok(term) = env::var("TERM") {
            term
        } else {
            return false;
        };
        if t == "dumb" {
            return false;
        }
        return true;
    }

    pub fn add_color(text: &str, component: LinePart) -> String {
        match component {
            LinePart::Path => Colour::Purple.paint(text).to_string(),
            LinePart::LineNumber => Colour::Green.paint(text).to_string(),
            LinePart::Match => Colour::Green.bold().paint(text).to_string(),
            LinePart::Separator => Colour::Cyan.paint(text).to_string(),
        }
    }
}

#[cfg(not(feature = "color"))]
mod color {
    use super::LinePart;
    pub fn is_color_output_available() -> bool {
        false
    }
    pub fn add_color(text: &str, _: LinePart) -> String {
        text.to_string()
    }
}

pub use color::{is_color_output_available, add_color};


fn main() {
    libcustomlogger::init(log::LogLevelFilter::Info).unwrap();

    let matches = clap::App::new("csearch")
                      .version(&crate_version!()[..])
                      .author("Vernon Jones <vernonrjones@gmail.com> (original code copyright \
                               2011 the Go authors)")
                      .about(ABOUT)
                      .arg(clap::Arg::with_name("PATTERN")
                               .help("a regular expression to search with")
                               .required(true)
                               .use_delimiter(false)
                               .index(1))
                      .arg(clap::Arg::with_name("count")
                               .short("c")
                               .long("count")
                               .help("print only a count of matching lines per file"))
                      .arg(clap::Arg::with_name("color")
                               .long("color")
                               .help("highlight matching strings")
                               .overrides_with("nocolor")
                               .hidden(!cfg!(feature = "color")))
                      .arg(clap::Arg::with_name("nocolor")
                               .long("nocolor")
                               .help("don't highlight matching strings")
                               .overrides_with("color")
                               .hidden(!cfg!(feature = "color")))
                      .arg(clap::Arg::with_name("FILE_PATTERN")
                               .short("G")
                               .long("file-search-regex")
                               .help("limit search to filenames matching FILE_PATTERN")
                               .takes_value(true))
                      .arg(clap::Arg::with_name("ignore-case")
                               .short("i")
                               .long("ignore-case")
                               .help("Match case insensitively"))
                      .arg(clap::Arg::with_name("files-with-matches")
                               .short("l")
                               .long("files-with-matches")
                               .help("Only print filenames that contain matches (don't print \
                                      the matching lines)"))
                      .arg(clap::Arg::with_name("line-number")
                               .short("n")
                               .long("line-number")
                               .help("print line number with output lines"))
                      .arg(clap::Arg::with_name("visual-studio-format")
                               .long("format-vs")
                               .help("print lines in a format that can be parsed by Visual \
                                      Studio 2008"))
                      .arg(clap::Arg::with_name("NUM")
                               .short("m")
                               .long("max-count")
                               .takes_value(true)
                               .help("stop after NUM matches"))
                      .arg(clap::Arg::with_name("bruteforce")
                               .long("brute")
                               .help("brute force - search all files in the index"))
                      .arg(clap::Arg::with_name("INDEX_FILE")
                               .long("indexpath")
                               .takes_value(true)
                               .help("use specified INDEX_FILE as the index path. overrides \
                                      $CSEARCHINDEX."))
                      .get_matches();

    // possibly add ignore case flag to the pattern
    let ignore_case = matches.is_present("ignore-case");

    // get the pattern provided by the user
    let pattern = {
        let user_pattern = matches.value_of("PATTERN").expect("Failed to get PATTERN");

        let ignore_case_flag = if ignore_case {
            "(?i)"
        } else {
            ""
        };
        let multiline_flag = "(?m)";
        String::from(ignore_case_flag) + multiline_flag + user_pattern
    };
    let regex_pattern = match regex::Regex::new(&pattern) {
        Ok(r) => r,
        Err(e) => panic!("PATTERN: {}", e),
    };

    // possibly override the csearchindex
    matches.value_of("INDEX_FILE").map(|p| {
        env::set_var("CSEARCHINDEX", p);
    });

    // combine cmdline options used for matching/output into a structure
    let match_options = MatchOptions {
        pattern: regex_pattern,
        print_format: if matches.is_present("visual-studio-format") {
            PrintFormat::VisualStudio
        } else {
            PrintFormat::Normal
        },
        print_count: matches.is_present("count"),
        ignore_case: ignore_case,
        files_with_matches_only: matches.is_present("files-with-matches"),
        line_number: matches.is_present("line-number") ||
                     matches.is_present("visual-studio-format"),
        with_color: !matches.is_present("nocolor") && is_color_output_available(),
        max_count: matches.value_of("NUM").map(|s| {
            match usize::from_str_radix(s, 10) {
                Ok(n) => n,
                Err(parse_err) => panic!("NUM: {}", parse_err),
            }
        }),
    };

    // Get the index from file
    let index_path = libcsearch::csearch_index();
    let index_reader = match IndexReader::open(index_path) {
        Ok(i) => i,
        Err(e) => panic!("{}", e),
    };

    // Find all possibly matching files using the pseudo-regexp
    let mut post = if matches.is_present("bruteforce") {
        index_reader.query(Query::all(), None)
    } else {
        // Get the pseudo-regexp (built using trigrams)
        let expr = regex_syntax::Expr::parse(&pattern).unwrap();
        let q = RegexInfo::new(expr).query;

        index_reader.query(q, None)
    };

    // If provided, filter possibly matching files via FILE_PATTERN
    if let Some(ref file_pattern_str) = matches.value_of("FILE_PATTERN") {
        let file_pattern = match regex::Regex::new(&file_pattern_str) {
            Ok(r) => r,
            Err(e) => panic!("FILE_PATTERN: {}", e),
        };
        post = post.into_iter()
                   .filter(|file_id| {
                       let name = index_reader.name(*file_id);
                       file_pattern.is_match(&name)
                   })
                   .collect::<HashSet<_>>();
    }

    // Search all possibly matching files for matches, printing the matching lines
    let g = grep::Grep::new(match_options.pattern.clone());
    let max_count = match_options.max_count.clone();
    let mut line_printer = LinePrinter::new(&match_options);
    let mut total_matches = 0;
    'files: for file_id in post {
        let name = index_reader.name(file_id);
        let g_it = match g.open(name.clone()) {
            Ok(g_it) => g_it,
            Err(cause) => {
                warn!("{} - File open failure: {}", name, cause);
                continue;
            }
        };
        for each_line in g_it {
            total_matches += 1;
            if let Some(ref m) = max_count {
                if *m != 0 && total_matches > *m {
                    break 'files;
                }
            }
            match line_printer.print_line(&name, &each_line) {
                Ok(_) => (),
                Err(_) => return,    // return early if stdout is closed
            }
        }
    }
    if match_options.print_count {
        let mut kv: Vec<_> = line_printer.num_matches.iter().collect();
        kv.sort();
        for (k, v) in kv {
            println!("{}: {}", maybe_make_relative(k).display(), v);
        }
    } else if match_options.files_with_matches_only {
        let mut v: Vec<_> = line_printer.num_matches.keys().collect();
        v.sort();
        for k in v {
            println!("{}", maybe_make_relative(k).display());
        }
    }

}

struct LinePrinter<'a> {
    options: &'a MatchOptions,
    num_matches: HashMap<PathBuf, usize>,
}


impl<'a> LinePrinter<'a> {
    fn new(options: &'a MatchOptions) -> Self {
        LinePrinter {
            options: options,
            num_matches: HashMap::new(),
        }
    }
    fn all_lines_printed(&self) -> bool {
        if self.options.print_count || self.options.files_with_matches_only {
            false
        } else {
            true
        }
    }
    fn only_filenames_printed(&self) -> bool {
        self.options.files_with_matches_only
    }
    fn increment_file_match<P: AsRef<Path>>(&mut self, filename: P) {
        if self.num_matches.contains_key(filename.as_ref()) {
            let mut n = self.num_matches
                            .get_mut(filename.as_ref())
                            .expect("expected filename key to exist");
            *n += 1;
        } else {
            self.num_matches.insert(PathBuf::from(filename.as_ref()), 1);
        }
    }
    fn print_line<P: AsRef<Path>>(&mut self,
                                  filename: P,
                                  result: &grep::MatchResult)
                                  -> io::Result<()> {
        self.increment_file_match(filename.as_ref());
        if self.all_lines_printed() {
            let out_line = self.format_line(filename, result);
            writeln!(&mut std::io::stdout(), "{}", out_line)
        } else if self.only_filenames_printed() {
            return Ok(());
        } else {
            return Ok(());
        }
    }
    fn format_line<P: AsRef<Path>>(&self, filename: P, result: &grep::MatchResult) -> String {
        let mut out_line = String::new();
        let simplified_path = maybe_make_relative(filename);
        let path_component = self.maybe_add_color(&format!("{}", simplified_path.display()),
                                                  LinePart::Path);
        out_line.push_str(&path_component);
        let start_sep = if self.options.print_format == PrintFormat::VisualStudio {
            "("
        } else {
            ":"
        };
        out_line.push_str(&self.maybe_add_color(start_sep, LinePart::Separator));
        if self.options.line_number {
            let line_number = self.maybe_add_color(&(result.line_number + 1).to_string(),
                                                   LinePart::LineNumber);
            out_line.push_str(&line_number);
            if self.options.print_format == PrintFormat::VisualStudio {
                out_line.push_str(&self.maybe_add_color(")", LinePart::Separator));
            }
            out_line.push_str(&self.maybe_add_color(":", LinePart::Separator));
        }
        let line = {
            let (start, end) = self.options.pattern.find(&result.line).unwrap();
            String::from(&result.line[0..start]) +
            &self.maybe_add_color(&result.line[start..end], LinePart::Match) +
            &result.line[end..]
        };
        out_line.push_str(&line);
        out_line
    }
    fn maybe_add_color(&self, text: &str, component: LinePart) -> String {
        if self.options.with_color {
            add_color(text, component)
        } else {
            text.to_string()
        }
    }
}

fn maybe_make_relative<P: AsRef<Path>>(p: P) -> PathBuf {
    PathBuf::from(p.as_ref()
                   .strip_prefix(&env::current_dir().unwrap())
                   .unwrap_or(p.as_ref()))
}
