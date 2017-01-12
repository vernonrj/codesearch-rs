extern crate regex_syntax;

extern crate libcsearch;

use regex_syntax::Expr;

use libcsearch::regexp::RegexInfo;

macro_rules! regex_eq {
    ( $r:expr, $expected:expr ) => {
        {
            let e = match Expr::parse($r.as_ref()) {
                Ok(e) => e,
                Err(e) => panic!("FAILED to parse expr `{}` --- CAUSE: {}", $r, e)
            };
            let regexinfo = RegexInfo::new(e).unwrap();
            println!("RegexInfo = {}", regexinfo.format_as_string());
            let q = regexinfo.query;
            println!("Query = {:?}", q);
            assert_eq!($expected.to_string(), q.format_as_string());
        };
    }
}

#[test]
fn test_query() {
    regex_eq!(r"Abcdef", "\"Abc\" \"bcd\" \"cde\" \"def\"");
    regex_eq!(r"(abc)(def)", "\"abc\" \"bcd\" \"cde\" \"def\"");
    regex_eq!(r"abc.*(def|ghi)", "\"abc\" (\"def\"|\"ghi\")");
    regex_eq!(r"abc(def|ghi)",
              "\"abc\" (\"bcd\" \"cde\" \"def\")|(\"bcg\" \"cgh\" \"ghi\")");
    regex_eq!(r"a+hello", "\"ahe\" \"ell\" \"hel\" \"llo\"");
    regex_eq!(r"(a+hello|b+world)",
              "(\"ahe\" \"ell\" \"hel\" \"llo\")|(\"bwo\" \"orl\" \"rld\" \"wor\")");
    regex_eq!(r"a*bbb", "\"bbb\"");
    regex_eq!(r"a?bbb", "\"bbb\"");
    regex_eq!(r"(bbb)a?", "\"bbb\"");
    regex_eq!(r"(bbb)a*", "\"bbb\"");
    regex_eq!(r"^abc", "\"abc\"");
    regex_eq!(r"abc$", "\"abc\"");
    regex_eq!(r"ab[cde]f",
              "(\"abc\" \"bcf\")|(\"abd\" \"bdf\")|(\"abe\" \"bef\")");
    regex_eq!(r"(abc|bac)de",
              "\"cde\" (\"abc\" \"bcd\")|(\"acd\" \"bac\")");
}

#[test]
fn test_case_insensitive() {
    RegexInfo::new(Expr::parse(r"(?i)abcd efgh").unwrap()).unwrap();
}

#[test]
fn test_space() {
    regex_eq!(r"\s", "+");
    // this is different from the go version because the rust
    // version considers unicode spaces too
    regex_eq!(r"a\sb", "+");

    regex_eq!(r"abc[ \t\na-zA-Z0-9]def", "\"abc\" \"def\"");
}

#[test]
fn test_digit() {
    regex_eq!(r"\d", "+");
    regex_eq!(r"\d+", "+");
}

#[test]
fn test_query_short() {
    // These don't have enough letters for a trigram, so they return the
    // always matching query \"+\".
    regex_eq!(r"ab[^cde]f", "+");
    regex_eq!(r"ab.f", "+");
    regex_eq!(r".", "+");
    // NOTE: the rust regex crate doesn't allow the empty group
    // regex_eq!(r"()", "+");
}

// #[test]
// fn test_query_no_matches() {
//     // No matches.
//     regex_eq!(r"[^\s\S]", "-");
// }

#[test]
fn test_query_factoring() {
    // Factoring works.
    regex_eq!(r"(abc|abc)", "\"abc\"");
    regex_eq!(r"(ab|ab)c", "\"abc\"");
    regex_eq!(r"ab(cab|cat)", "\"abc\" \"bca\" (\"cab\"|\"cat\")");
    regex_eq!(r"(z*(abc|def)z*)(z*(abc|def)z*)", "(\"abc\"|\"def\")");
    regex_eq!(r"(z*abcz*defz*)|(z*abcz*defz*)", "\"abc\" \"def\"");
    regex_eq!(r"(z*abcz*defz*(ghi|jkl)z*)|(z*abcz*defz*(mno|prs)z*)",
              "\"abc\" \"def\" (\"ghi\"|\"jkl\"|\"mno\"|\"prs\")");
    regex_eq!(r"(z*(abcz*def)|(ghiz*jkl)z*)|(z*(mnoz*prs)|(tuvz*wxy)z*)",
              "(\"abc\" \"def\")|(\"ghi\" \"jkl\")|(\"mno\" \"prs\")|(\"tuv\" \"wxy\")");
    regex_eq!(r"(z*abcz*defz*)(z*(ghi|jkl)z*)",
              "\"abc\" \"def\" (\"ghi\"|\"jkl\")");
    regex_eq!(r"(z*abcz*defz*)|(z*(ghi|jkl)z*)",
              "(\"ghi\"|\"jkl\")|(\"abc\" \"def\")");
}

#[test]
fn test_query_prefix_suffix() {
    // analyze keeps track of multiple possible prefix/suffixes.
    regex_eq!(r"[ab][cd][ef]",
              "(\"ace\"|\"acf\"|\"ade\"|\"adf\"|\"bce\"|\"bcf\"|\"bde\"|\"bdf\")");
    regex_eq!(r"ab[cd]e", "(\"abc\" \"bce\")|(\"abd\" \"bde\")");

    // Different sized suffixes.
    regex_eq!(r"(a|ab)cde", "\"cde\" (\"abc\" \"bcd\")|(\"acd\")");
    regex_eq!(r"(a|b|c|d)(ef|g|hi|j)", "+");

    regex_eq!(r"(?s).", "+");
}

#[test]
fn test_query_expand_case() {
    // Expanding case.
    regex_eq!(r"(?i)a~~", "(\"A~~\"|\"a~~\")");
    regex_eq!(r"(?i)ab~", "(\"AB~\"|\"Ab~\"|\"aB~\"|\"ab~\")");
    regex_eq!(r"(?i)abc",
              "(\"ABC\"|\"ABc\"|\"AbC\"|\"Abc\"|\"aBC\"|\"aBc\"|\"abC\"|\"abc\")");
    regex_eq!(r"(?i)abc|def",
              "(\"ABC\"|\"ABc\"|\"AbC\"|\"Abc\"|\"DEF\"|\"DEf\"|\"DeF\"|\"Def\"\
              |\"aBC\"|\"aBc\"|\"abC\"|\"abc\"|\"dEF\"|\"dEf\"|\"deF\"|\"def\")");
    regex_eq!(r"(?i)abcd",
              "(\"ABC\"|\"ABc\"|\"AbC\"|\"Abc\"|\"aBC\"|\"aBc\"|\"abC\"|\"abc\") \
               (\"BCD\"|\"BCd\"|\"BcD\"|\"Bcd\"|\"bCD\"|\"bCd\"|\"bcD\"|\"bcd\")");
    regex_eq!(r"(?i)abc|abc",
              "(\"ABC\"|\"ABc\"|\"AbC\"|\"Abc\"|\"aBC\"|\"aBc\"|\"abC\"|\"abc\")");
}

#[test]
fn test_query_word_boundary() {
    // Word boundary.
    regex_eq!(r"\b", "+");
    regex_eq!(r"\B", "+");
    regex_eq!(r"\babc", "\"abc\"");
    regex_eq!(r"\Babc", "\"abc\"");
    regex_eq!(r"abc\b", "\"abc\"");
    regex_eq!(r"abc\B", "\"abc\"");
    regex_eq!(r"ab\bc", "\"abc\"");
    regex_eq!(r"ab\Bc", "\"abc\"");
}
