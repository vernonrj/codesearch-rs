// Copyright 2015 Vernon Jones.
// Original code Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
use std::char;
use std::collections::BTreeSet;

pub type StringSet = BTreeSet<String>;

// use regex::Regex;
use regex_syntax::{Expr, Repeater, CharClass, ClassRange};

/// Operation on a Query
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum QueryOperation {
    All,
    None,
    And,
    Or,
}

pub type Trigram = String;

/// A structure, similar to a regular expression, that uses
/// composed trigrams to find matches in text.
#[derive(Debug, Clone)]
pub struct Query {
    pub operation: QueryOperation,
    pub trigram: BTreeSet<Trigram>,
    pub sub: Vec<Query>,
}

impl Query {
    pub fn format_as_string(&self) -> String {
        let mut s = String::new();
        match self.operation {
            QueryOperation::None => return String::from("-"),
            QueryOperation::All => return String::from("+"),
            _ => ()
        }
        if self.sub.is_empty() && self.trigram.len() == 1 {
            s.push_str(&format!("\"{}\"", self.trigram.iter().next().unwrap()));
            return s;
        }
        let (sjoin, tjoin) = match self.operation {
            QueryOperation::And => (" ", " "),
            _ => (")|(", "|")
        };
        let end = if self.operation ==  QueryOperation::And {
            ""
        } else {
            s.push_str("(");
            ")"
        };
        let mut trigrams = self.trigram.iter().cloned().collect::<Vec<_>>();
        trigrams.sort();
        for (i, t) in trigrams.into_iter().enumerate() {
            if i > 0 {
                s.push_str(tjoin);
            }
            s.push_str(&format!("\"{}\"", t));
        }
        if !self.sub.is_empty() {
            if !self.trigram.is_empty() {
                s.push_str(sjoin);
            }
            s.push_str(&self.sub[0].format_as_string());
            for elem in &self.sub[1..] {
                s.push_str(&format!("{}{}", sjoin, elem.format_as_string()));
            }
        }
        s.push_str(end);
        s
    }
}

impl Default for Query {
    fn default() -> Self {
        Query {
            operation: QueryOperation::All,
            trigram: BTreeSet::new(),
            sub: Vec::new(),
        }
    }
}

impl Query {
    pub fn new(operation: QueryOperation) -> Query {
        Query {
            operation: operation,
            trigram: BTreeSet::new(),
            sub: Vec::new(),
        }
    }
    pub fn all() -> Query {
        Query::new(QueryOperation::All)
    }
    pub fn none() -> Query {
        Query::new(QueryOperation::None)
    }
    pub fn implies(&self, rhs: &Query) -> bool {
        match (self.operation, rhs.operation) {
            (QueryOperation::None, _) | (_, QueryOperation::All) => {
                // False implies everything.
                // Everything implies True.
                return true;
            }
            (QueryOperation::All, _) | (_, QueryOperation::None) => {
                // True implies nothing.
                // Nothing implies False.
                return false;
            }
            (_, _) => (),
        }
        if self.operation == QueryOperation::And ||
           (self.operation == QueryOperation::Or && self.trigram.len() == 1 &&
            self.sub.len() == 0) {
            return trigrams_imply(&self.trigram, rhs);
        }
        if self.operation == QueryOperation::Or && rhs.operation == QueryOperation::Or &&
           self.trigram.len() > 0 && self.sub.len() == 0 &&
           self.trigram.is_subset(&rhs.trigram) {
            return true;
        }
        return false;
    }
    pub fn and(self, rhs: Query) -> Query {
        and_or(self, rhs, QueryOperation::And)
    }
    pub fn or(self, rhs: Query) -> Query {
        and_or(self, rhs, QueryOperation::Or)
    }
    fn simplify(mut self) -> Query {
        if self.trigram.len() == 0 && self.sub.len() == 1 {
            self.sub.pop().unwrap()
        } else {
            self
        }
    }
    pub fn is_atom(&self) -> bool {
        self.trigram.len() == 1 && self.sub.is_empty()
    }
}

#[derive(Default, Debug)]
pub struct RegexInfo {
    pub can_empty: bool,
    pub exact_set: Option<StringSet>,
    pub prefix: StringSet,
    pub suffix: StringSet,
    pub query: Query,
}

impl RegexInfo {
    pub fn new(expr: Expr) -> Self {
        match expr {
            Expr::Empty |
            Expr::StartLine |
            Expr::EndLine |
            Expr::StartText |
            Expr::EndText |
            Expr::WordBoundary |
            Expr::NotWordBoundary => Self::empty_string(),
            Expr::Literal {chars, casei: true} => {
                match chars.len() {
                    0 => Self::empty_string(),
                    1 => {
                        let re1 = Expr::Class(CharClass::new(vec![ClassRange {
                                                                      start: chars[0],
                                                                      end: chars[0],
                                                                  }]));
                        RegexInfo::new(re1)
                    }
                    _ => {
                        // Multi-letter case-folded string:
                        // treat as concatenation of single-letter case-folded strings.
                        chars.iter().fold(Self::empty_string(), |info, c| {
                            concat(info,
                                   Self::new(Expr::Literal {
                                       chars: vec![*c],
                                       casei: true,
                                   }))
                        })
                    }
                }
            }
            Expr::Literal {chars, casei: false} => {
                println!("literal {:?}", chars);
                let exact_set = {
                    let mut h = StringSet::new();
                    h.insert(chars.into_iter().collect());
                    h
                };
                let r = RegexInfo {
                    can_empty: false,
                    exact_set: Some(exact_set.clone()),
                    prefix: StringSet::new(),
                    suffix: StringSet::new(),
                    query: Query::all()
                };
                simplify(r, false)
            }
            Expr::AnyChar | Expr::AnyCharNoNL => Self::any_char(),
            Expr::Concat(exprs) => {
                println!("OpConcat");
                if exprs.is_empty() {
                    return Self::empty_string();
                }
                let mut exprs_it = exprs.into_iter().map(RegexInfo::new);
                let first = exprs_it.next().unwrap();
                exprs_it.fold(first, concat)
            }
            Expr::Alternate(v) => {
                println!("OpAlternate");
                if v.is_empty() {
                    return Self::no_match();
                }
                let mut analyzed = v.into_iter().map(RegexInfo::new);
                let first = analyzed.next().unwrap();
                analyzed.fold(first, alternate)
            }
            Expr::Repeat {e, r, /* ref greedy */ .. } => {
                println!("OpRepeat");
                match r {
                    Repeater::ZeroOrOne => alternate(RegexInfo::new(*e), Self::empty_string()),
                    Repeater::ZeroOrMore | Repeater::Range {..} => {
                        // We don't know anything, so assume the worst.
                        Self::any_match()
                    },
                    Repeater::OneOrMore => {
                        // x+
                        // Since there has to be at least one x, the prefixes and suffixes
                        // stay the same.  If x was exact, it isn't anymore.

                        let mut info = RegexInfo::new(*e);
                        if let Some(i_s) = info.exact_set {
                            info.prefix = i_s.clone();
                            info.suffix = i_s;
                            info.exact_set = None;
                        }
                        simplify(info, false)
                    },
                }
            }
            Expr::Class(ref ranges) if ranges.is_empty() => Self::no_match(),
            Expr::Class(ref ranges) => {
                let mut info = RegexInfo {
                    can_empty: false,
                    exact_set: None,
                    prefix: StringSet::new(),
                    suffix: StringSet::new(),
                    query: Query::all(),
                };
                for each_range in ranges {
                    let &ClassRange { start, end } = each_range;
                    // if the class is too large, it's okay to overestimate
                    if (end as u32 - start as u32) > 100 {
                        return Self::any_char();
                    }
                    let next_range: StringSet = {
                        let mut h = StringSet::new();
                        let it = CharRangeIter::new(start, end).expect("expected valid range");
                        for chr in it {
                            let mut s = String::new();
                            s.push(chr);
                            h.insert(s);
                        }
                        h
                    };
                    if let Some(ref mut exact) = info.exact_set {
                        *exact = union(&exact, &next_range);
                    } else {
                        info.exact_set = Some(next_range);
                    }
                }
                simplify(info, false)
            },
            Expr::Group { e, .. } => {
                println!("group");
                RegexInfo::new(*e)
            },
        }
    }
    fn no_match() -> Self {
        RegexInfo {
            can_empty: false,
            exact_set: None,
            prefix: StringSet::new(),
            suffix: StringSet::new(),
            query: Query::new(QueryOperation::None),
        }
    }
    fn empty_string() -> Self {
        RegexInfo {
            can_empty: true,
            exact_set: Some(Self::hashset_with_only_emptystring()),
            prefix: StringSet::new(),
            suffix: StringSet::new(),
            query: Query::all(),
        }
    }
    fn any_char() -> Self {
        RegexInfo {
            can_empty: false,
            exact_set: None,
            prefix: Self::hashset_with_only_emptystring(),
            suffix: Self::hashset_with_only_emptystring(),
            query: Query::all(),
        }
    }
    fn any_match() -> Self {
        RegexInfo {
            can_empty: true,
            exact_set: None,
            prefix: Self::hashset_with_only_emptystring(),
            suffix: Self::hashset_with_only_emptystring(),
            query: Query::new(QueryOperation::All),
        }
    }
    fn hashset_with_only_emptystring() -> StringSet {
        let mut h = StringSet::new();
        h.insert("".to_string());
        h
    }
    pub fn format_as_string(&self) -> String {
        let mut s = String::new();
        if self.can_empty {
            s.push_str("canempty ");
        }
        if let Some(ref exact) = self.exact_set {
            s.push_str("exact: ");
            s.push_str(&(&exact.iter().cloned().collect::<Vec<_>>()[..]).join(","));
        } else {
            s.push_str("prefix: ");
            s.push_str(&(&self.prefix.iter().cloned().collect::<Vec<_>>()[..]).join(","));
            s.push_str(" suffix: ");
            s.push_str(&(&self.suffix.iter().cloned().collect::<Vec<_>>()[..]).join(","));
        }
        s.push_str(&format!(" match: {}", self.query.format_as_string()));
        s
    }
}

fn concat(x: RegexInfo, y: RegexInfo) -> RegexInfo {
    println!("concat {} ... {}", x.format_as_string(), y.format_as_string());
    let mut xy = RegexInfo::default();

    if let (&Some(ref x_s), &Some(ref y_s)) = (&x.exact_set, &y.exact_set) {
        println!("if case {:?}, {:?}", x_s, y_s);
        xy.exact_set = Some(cross_product(&x_s, &y_s));
    } else {
        println!("else case");
        if let &Some(ref x_s) = &x.exact_set {
            xy.prefix = cross_product(&x_s, &y.prefix);
        } else {
            xy.prefix = if x.can_empty {
                union(&x.prefix, &y.prefix)
            } else {
                x.prefix
            };
        }
        if let &Some(ref y_s) = &y.exact_set {
            xy.suffix = cross_product(&x.suffix, &y_s);
        } else {
            xy.suffix = if y.can_empty {
                union(&y.suffix, &x.suffix)
            } else {
                y.suffix
            }
        }
    }

    // If all the possible strings in the cross product of x.suffix
    // and y.prefix are long enough, then the trigram for one
    // of them must be present and would not necessarily be
    // accounted for in xy.prefix or xy.suffix yet.  Cut things off
    // at maxSet just to keep the sets manageable.
    xy.query = if x.exact_set.is_none() && y.exact_set.is_none() && x.suffix.len() <= 20 &&
                  y.prefix.len() <= 20 &&
                  (min_string_len(&x.suffix) + min_string_len(&y.prefix)) >= 3 {
        println!("second if case");
        and_trigrams(xy.query, &cross_product(&x.suffix, &y.prefix))
    } else {
        println!("second else case");
        x.query.and(y.query)
    };

    println!("concat: before simplify: {:?}", xy.format_as_string());
    xy = simplify(xy, false);
    println!("concat: after simplify: {:?}", xy.format_as_string());
    xy
}

/// Returns the RegexInfo for x|y given x and y
fn alternate(x: RegexInfo, y: RegexInfo) -> RegexInfo {
    println!("alternate");
    let mut x = x;
    let mut y = y;
    let mut xy = RegexInfo::default();
    let mut add_exact_x = false;
    let mut add_exact_y = false;
    match (&x.exact_set, &y.exact_set) {
        (&Some(ref x_s), &Some(ref y_s)) => {
            xy.exact_set = Some(union(&x_s, y_s));
        }
        (&Some(ref x_s), &None) => {
            xy.prefix = union(&x_s, &y.prefix);
            xy.suffix = union(&x_s, &y.suffix);
            add_exact_x = true;
        }
        (&None, &Some(ref y_s)) => {
            xy.prefix = union(&x.prefix, &y_s);
            xy.suffix = union(&x.suffix, &y_s);
            add_exact_y = true;
        }
        _ => {
            xy.prefix = union(&x.prefix, &y.prefix);
            xy.suffix = union(&x.suffix, &y.suffix);
        }
    }
    if add_exact_x { add_exact(&mut x); }
    if add_exact_y { add_exact(&mut y); }
    xy.can_empty = x.can_empty || y.can_empty;
    xy.query = x.query.or(y.query);
    xy
}

fn add_exact(x: &mut RegexInfo) {
    println!("add_exact");
    let exact = if let Some(ref exact) = x.exact_set {
        exact.clone()
    } else {
        return;
    };
    x.query = and_trigrams(x.query.clone(), &exact);
}

fn simplify(mut info: RegexInfo, force: bool) -> RegexInfo {
    println!("  simplify {} force={}", info.format_as_string(), force);
    let do_simplify = if let Some(ref exact) = info.exact_set {
        exact.len() > 7 
            || (min_string_len(&exact) >= 3 && force)
            || min_string_len(&exact) >= 4
    } else {
        false
    };

    println!("simplify? {}", do_simplify);
    if do_simplify {
        add_exact(&mut info);
    }


    if let Some(ref exact) = info.exact_set {
        if exact.len() > 7 
            || (min_string_len(&exact) >= 3 && force)
            || min_string_len(&exact) >= 4
        {
            for s in exact {
                let n = s.len();
                if n < 3 {
                    info.prefix.insert(s.clone());
                    info.suffix.insert(s.clone());
                } else {
                    info.prefix.insert(s[..2].to_string());
                    info.suffix.insert(s[n-2..].to_string());
                }
            }
        }
    }
    if do_simplify {
        info.exact_set = None;
    }

    if info.exact_set.is_none() {
        info.query = simplify_set(info.query, &mut info.prefix, false);
        info.query = simplify_set(info.query, &mut info.suffix, true);
    }
    info
}

fn simplify_set(mut q: Query, prefix_or_suffix: &mut StringSet, is_suffix: bool) -> Query {
    println!("simplify_set");
    q = and_trigrams(q, prefix_or_suffix);
    println!("simplify_set: now match = {}", q.format_as_string());
    let mut t = StringSet::new();
    let mut n = 3;
    while n == 3 || prefix_or_suffix.len() > 20 {
        for string in prefix_or_suffix.iter() {
            let mut s: &str = &string;
            if s.len() > n {
                s = if !is_suffix {
                    &s[..n-1]
                } else {
                    &s[s.len()-n+1..]
                };
            }
            t.insert(s.to_string());
        }
        n -= 1;
    }
    // Now make sure that the prefix/suffix sets aren't redundant.
    // For example, if we know "ab" is a possible prefix, then it
    // doesn't help at all to know that  "abc" is also a possible
    // prefix, so delete "abc".

    // let f = if is_suffix {
    //     |a, b| b.is_suffix_of(a)
    // } else {
    //     |a, b| b.is_prefix_of(a)
    // };

    *prefix_or_suffix = t;
    q
}

/// Returns the length of the shortest string in xs
fn min_string_len(xs: &StringSet) -> usize {
    xs.iter().map(String::len).min().unwrap()
}

/// Returns the cross product of s and t
fn cross_product(s: &BTreeSet<Trigram>, t: &BTreeSet<Trigram>) -> BTreeSet<Trigram> {
    println!("cross");
    let mut p = BTreeSet::new();
    for s_string in s {
        for t_string in t {
            let mut cross_string = s_string.clone();
            cross_string.push_str(&t_string);
            println!("add {} to {:?}", cross_string, p);
            p.insert(cross_string);
        }
    }
    p
}

fn trigrams_imply(trigram: &BTreeSet<Trigram>, rhs: &Query) -> bool {
    match rhs.operation {
        QueryOperation::Or => {
            if rhs.sub.iter().any(|s| trigrams_imply(trigram, s)) {
                return true;
            }
            if trigram.iter().any(|s| rhs.trigram.contains(s)) {
                return true;
            }
            return false;
        }
        QueryOperation::And => {
            if !rhs.sub.iter().any(|s| trigrams_imply(trigram, s)) {
                return false;
            }
            if !trigram.iter().any(|s| rhs.trigram.contains(s)) {
                return false;
            }
            return true;
        }
        _ => false,
    }
}

fn and_trigrams(q: Query, t: &BTreeSet<Trigram>) -> Query {
    println!("and_trigrams");
    if min_string_len(t) < 3 {
        // If there is a short string, we can't guarantee
        // that any trigrams must be present, so use ALL.
        // q AND ALL = q.
        println!("and_trigrams: min string too short: {}", min_string_len(t));
        return q;
    }
    let or = t.iter().fold(Query::none(), |or, t_string| {
        println!("and_trigrams: work with {}", t_string);
        let mut trigram = BTreeSet::<Trigram>::new();
        // NOTE: the .windows() slice method would be better here,
        //       but it doesn't seem to be available for chars
        for i in 0..(t_string.len() - 2) {
            trigram.insert(t_string[i..i + 3].to_string());
        }
        or.or(Query {
            operation: QueryOperation::And,
            trigram: trigram,
            sub: Vec::new(),
        })
    });
    q.and(or)
}

/**
 * returns self OP other, possibly reusing self and other's storage.
 */
fn and_or(q: Query, r: Query, operation: QueryOperation) -> Query {
    let mut q = q.simplify();
    let mut r = r.simplify();

    // Boolean simplification.
    // If q ⇒ r, q AND r ≡ q.
    // If q ⇒ r, q OR r ≡ r.
    if q.implies(&r) {
        println!("{} implies {}", q.format_as_string(), r.format_as_string());
        if operation == QueryOperation::And {
            return q;
        } else {
            return r;
        }
    }
    if r.implies(&q) {
        println!("{} implies {}", r.format_as_string(), q.format_as_string());
        if operation == QueryOperation::And {
            return r;
        } else {
            return q;
        }
    }
    // Both q and r are QAnd or QOr.
    // If they match or can be made to match, merge.
    if q.operation == operation && (r.operation == operation || r.is_atom()) {
        println!("union! {:?} {:?}", q, r);
        q.trigram = union(&q.trigram, &r.trigram);
        q.sub.append(&mut r.sub);
        println!("now it's {:?}", q);
        return q;
    }
    if r.operation == operation && q.is_atom() {
        r.trigram = union(&r.trigram, &q.trigram);
        return r;
    }
    if q.is_atom() && r.is_atom() {
        q.operation = operation;
        q.trigram = union(&q.trigram, &r.trigram);
        return q;
    }
    // If one matches the op, add the other to it.
    if q.operation == operation {
        q.sub.push(r);
        return q;
    }
    if r.operation == operation {
        r.sub.push(q);
        return r;
    }
    // We are creating an AND of ORs or an OR of ANDs.
    // Factor out common trigrams, if any.
    let common = intersection(&q.trigram, &r.trigram);
    q.trigram = difference(&q.trigram, &common);
    r.trigram = difference(&r.trigram, &common);
    if !common.is_empty() {
        // If there were common trigrams, rewrite
        //
        //    (abc|def|ghi|jkl) AND (abc|def|mno|prs) =>
        //        (abc|def) OR ((ghi|jkl) AND (mno|prs))
        //
        //    (abc&def&ghi&jkl) OR (abc&def&mno&prs) =>
        //        (abc&def) AND ((ghi&jkl) OR (mno&prs))
        //
        // Build up the right one of
        //    (ghi|jkl) AND (mno|prs)
        //    (ghi&jkl) OR (mno&prs)
        // Call andOr recursively in case q and r can now be simplified
        // (we removed some trigrams).
        let s = and_or(q, r, operation);
        let new_operation = match operation {
            QueryOperation::And => QueryOperation::Or,
            QueryOperation::Or => QueryOperation::And,
            _ => panic!("unexpected query operation: {:?}", operation),
        };
        let t = Query {
            operation: new_operation,
            trigram: common,
            sub: Vec::new(),
        };
        and_or(t, s, new_operation)
    } else {
        // Otherwise just create the op.
        Query {
            operation: operation,
            trigram: BTreeSet::new(),
            sub: vec![q, r],
        }
    }
}


struct CharRangeIter {
    #[allow(dead_code)]
    low: char, // here to make debugging easier
    high: char,
    position: char,
}

impl CharRangeIter {
    fn new(low: char, high: char) -> Option<Self> {
        if (low as u32) > (high as u32) {
            None
        } else if low > char::MAX || high > char::MAX {
            None
        } else {
            Some(CharRangeIter {
                low: low,
                high: high,
                position: low,
            })
        }
    }
}

impl Iterator for CharRangeIter {
    type Item = char;
    fn next(&mut self) -> Option<Self::Item> {
        if (self.position as u32) > (self.high as u32) {
            None
        } else {
            let old_position = self.position;
            self.position = char::from_u32(self.position as u32 + 1).unwrap();
            Some(old_position)
        }
    }
}

fn union<T: Eq + Ord + Clone>(s: &BTreeSet<T>, t: &BTreeSet<T>) -> BTreeSet<T> {
    s.union(t).cloned().collect()
}

fn intersection<T: Eq + Ord + Clone>(s: &BTreeSet<T>, t: &BTreeSet<T>) -> BTreeSet<T> {
    s.intersection(t).cloned().collect()
}

fn difference<T: Eq + Ord + Clone>(s: &BTreeSet<T>, t: &BTreeSet<T>) -> BTreeSet<T> {
    s.difference(t).cloned().collect()
}
