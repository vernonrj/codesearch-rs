
extern crate byteorder;
#[macro_use]
extern crate clap;
extern crate memmap;
extern crate num;
extern crate regex;
extern crate regex_syntax;
extern crate varint;

mod index;
mod grep;

use std::io::Write;

#[derive(Debug)]
pub struct MatchOptions {
    pub pattern: regex::Regex,
    pub print_count: bool,
    pub ignore_case: bool,
    pub files_with_matches_only: bool,
    pub line_number: bool,
    pub max_count: Option<usize>
}

fn main() {
    let matches = clap::App::new("csearch")
        .version(&crate_version!()[..])
        .author("Vernon Jones <vernonrjones@gmail.com> (original code copyright the Go authors)")
        .about("
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
")
        .arg(clap::Arg::with_name("PATTERN")
             .help("a regular expression to search with")
             .required(true)
             .index(1))
        .arg(clap::Arg::with_name("count")
             .short("c").long("count")
             .help("print only a count of matching lines per file"))
        .arg(clap::Arg::with_name("FILE_PATTERN")
             .short("G").long("file-search-regex")
             .help("limit search to filenames matching FILE_PATTERN")
             .takes_value(true))
        .arg(clap::Arg::with_name("ignore-case")
             .short("i").long("ignore-case")
             .help("Match case insensitively"))
        .arg(clap::Arg::with_name("files-with-matches")
             .short("l").long("files-with-matches")
             .help("Only print filenames that contain matches (don't print the matching lines)"))
        .arg(clap::Arg::with_name("line-number")
             .short("n").long("line-number")
             .help("print line number with output lines"))
        .arg(clap::Arg::with_name("NUM")
             .short("m").long("max-count")
             .takes_value(true)
             .help("stop after NUM matches"))
        .arg(clap::Arg::with_name("bruteforce")
             .long("brute")
             .help("brute force - search all files in the index"))
        .get_matches();

    // get the pattern provided by the user
    let mut pattern: String = matches.value_of("PATTERN").unwrap().to_string();
    pattern = "(?m)".to_string() + &pattern;

    // possibly add ignore case flag to the pattern
    let ignore_case = matches.is_present("ignore-case");
    if ignore_case {
        pattern = "(?i)".to_string() + &pattern;
    }

    // combine cmdline options used for matching/output into a structure
    let match_options = MatchOptions {
        // TODO: Catch bad regex earlier, maybe print a nice message
        pattern: regex::Regex::new(&pattern.clone()).expect("Invalid pattern supplied!"),
        print_count: matches.is_present("count"),
        ignore_case: ignore_case,
        files_with_matches_only: matches.is_present("files-with-matches"),
        line_number: matches.is_present("line-number"),
        max_count: matches.value_of("NUM").map(|s| usize::from_str_radix(s, 10).unwrap())
    };

    // Get the index from file
    // TODO: don't hardcode index location
    let i = index::read::Index::open("/home/vernon/.csearchindex").unwrap();

    // Get the pseudo-regexp (built using trigrams)
    let expr = regex_syntax::Expr::parse(&pattern.clone()).unwrap();
    let q = index::regexp::RegexInfo::new(&expr).query;

    // Find all possibly matching files using the pseudo-regexp
    let mut post = i.query(q, None);

    // If provided, filter possibly matching files via FILE_PATTERN
    if let Some(ref file_pattern_str) = matches.value_of("FILE_PATTERN") {
        let file_pattern = regex::Regex::new(&file_pattern_str)
            .expect("Invalid file pattern supplied!");
        post = post.iter().filter(|&file_id| {
            let name = i.name(*file_id as usize);
            file_pattern.is_match(&name)
        }).cloned().collect::<Vec<_>>();
    }

    // Search all possibly matching files for matches, printing the matching lines
    let g = grep::grep::Grep::new(match_options.pattern.clone());
    let mut line_printer = LinePrinter::new(match_options);
    for file_id in post {
        let name = i.name(file_id as usize);
        let maybe_g_it = g.open(name.clone());
        let g_it = if let Ok(g_it) = maybe_g_it {
            g_it
        } else if let Err(cause) = maybe_g_it {
            writeln!(&mut std::io::stderr(), "File open failure: {}", cause).unwrap();
            continue;
        } else {
            panic!("Ok, Err have been covered, but result is neither!");
        };
        for each_line in g_it {
            line_printer.print_line(&name, &each_line);
        }
    }

}

struct LinePrinter {
    options: MatchOptions,
    num_matches: usize
}


impl LinePrinter {
    fn new(options: MatchOptions) -> Self {
        LinePrinter {
            options: options,
            num_matches: 0
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
    fn print_line(&mut self, filename: &str, result: &grep::grep::MatchResult) {
        self.num_matches += 1;
        if self.all_lines_printed() {
            let mut out_line = String::new();
            out_line.push_str(filename);
            out_line.push_str(":");
            if self.options.line_number {
                out_line.push_str(&(result.line_number + 1).to_string()); // 0-based to 1-based
                out_line.push_str(":");
            }
            out_line.push_str(&result.line);
            println!("{}", out_line);
        } else if self.only_filenames_printed() {
            unimplemented!();
        } else {
            return;
        }
    }
}


