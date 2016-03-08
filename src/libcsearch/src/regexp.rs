// Copyright 2015 Vernon Jones.
// Original code Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
use std::char;
use std::collections::HashSet;
use std::ops::Deref;
use std::hash::Hash;

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
#[derive(Debug)]
pub struct Query {
    pub operation: QueryOperation,
    pub trigram: HashSet<Trigram>,
    pub sub: Vec<Query>,
}

impl Default for Query {
    fn default() -> Self {
        Query {
            operation: QueryOperation::All,
            trigram: HashSet::new(),
            sub: Vec::new(),
        }
    }
}

impl Query {
    pub fn new(operation: QueryOperation) -> Query {
        Query {
            operation: operation,
            trigram: HashSet::new(),
            sub: Vec::new(),
        }
    }
    // pub fn from_regex(expr: Regex) -> Query {
    //     RegexInfo::new(expr)
    //         .simplify(true)
    //         .add_exact()
    //         .query
    // }
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
    pub exact_set: Option<HashSet<String>>,
    pub prefix: HashSet<String>,
    pub suffix: HashSet<String>,
    pub query: Query,
}

impl RegexInfo {
    pub fn new(expr: &Expr) -> Self {
        match expr {
            &Expr::Empty |
            &Expr::StartLine |
            &Expr::EndLine |
            &Expr::StartText |
            &Expr::EndText |
            &Expr::WordBoundary |
            &Expr::NotWordBoundary => Self::empty_string(),
            &Expr::Literal {ref chars, casei: true} => {
                match chars.len() {
                    0 => Self::empty_string(),
                    1 => {
                        let re1 = Expr::Class(CharClass::new(vec![ClassRange {
                                                                      start: chars[0],
                                                                      end: chars[0],
                                                                  }]));
                        RegexInfo::new(&re1)
                    }
                    _ => {
                        // Multi-letter case-folded string:
                        // treat as concatenation of single-letter case-folded strings.
                        chars.iter().fold(Self::empty_string(), |info, c| {
                            concat(info,
                                   Self::new(&Expr::Literal {
                                       chars: vec![*c],
                                       casei: true,
                                   }))
                        })
                    }
                }
            }
            &Expr::Literal {ref chars, casei: false} => {
                let exact_set = {
                    let mut h = HashSet::<String>::new();
                    h.insert(chars.iter().cloned().collect());
                    h
                };
                RegexInfo {
                    can_empty: false,
                    exact_set: Some(exact_set.clone()),
                    prefix: HashSet::new(),
                    suffix: HashSet::new(),
                    query: and_trigrams(Query::all(), &exact_set),
                }
            }
            &Expr::AnyChar | &Expr::AnyCharNoNL => Self::any_char(),
            &Expr::Concat(ref v) => {
                let analyzed = v.iter().map(RegexInfo::new);
                analyzed.fold(Self::empty_string(), concat)
            }
            &Expr::Alternate(ref v) => {
                let analyzed = v.iter().map(RegexInfo::new);
                analyzed.fold(Self::no_match(), alternate)
            }
            &Expr::Repeat {ref e, ref r, /* ref greedy */ .. } => {
                match r {
                    &Repeater::ZeroOrOne => alternate(RegexInfo::new(e), Self::empty_string()),
                    &Repeater::ZeroOrMore => {
                        // We don't know anything, so assume the worst.
                        Self::any_match()
                    }
                    &Repeater::OneOrMore => {
                        // x+
                        // Since there has to be at least one x, the prefixes and suffixes
                        // stay the same.  If x was exact, it isn't anymore.

                        let mut info = RegexInfo::new(e);
                        if let Some(i_s) = info.exact_set {
                            info.prefix = i_s.clone();
                            info.suffix = i_s;
                            info.exact_set = None;
                        }
                        info
                    }
                    &Repeater::Range {..} => unimplemented!(), /* is this needed? */
                }
            }
            &Expr::Class(ref charclass) if charclass.is_empty() => Self::no_match(),
            &Expr::Class(ref charclass) => {
                let ranges = charclass.deref();
                let mut info = RegexInfo {
                    can_empty: false,
                    exact_set: None,
                    prefix: HashSet::new(),
                    suffix: HashSet::new(),
                    query: Query::all(),
                };
                for each_range in ranges {
                    let &ClassRange { start: low, end: high } = each_range;
                    let next_range: HashSet<String> = {
                        let mut h = HashSet::new();
                        let it = CharRangeIter::new(low, high).expect("expected valid range");
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
                info
            }
            _ => unimplemented!(), /* Still have more cases to implement */
        }
    }
    fn no_match() -> Self {
        RegexInfo {
            can_empty: false,
            exact_set: None,
            prefix: HashSet::new(),
            suffix: HashSet::new(),
            query: Query::new(QueryOperation::None),
        }
    }
    fn empty_string() -> Self {
        RegexInfo {
            can_empty: true,
            exact_set: Some(Self::hashset_with_only_emptystring()),
            prefix: HashSet::new(),
            suffix: HashSet::new(),
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
    fn hashset_with_only_emptystring() -> HashSet<String> {
        let mut h = HashSet::new();
        h.insert("".to_string());
        h
    }
}

fn concat(x: RegexInfo, y: RegexInfo) -> RegexInfo {
    let mut xy = RegexInfo::default();

    // If all the possible strings in the cross product of x.suffix
    // and y.prefix are long enough, then the trigram for one
    // of them must be present and would not necessarily be
    // accounted for in xy.prefix or xy.suffix yet.  Cut things off
    // at maxSet just to keep the sets manageable.
    xy.query = if x.exact_set.is_none() && y.exact_set.is_none() && x.suffix.len() <= 20 &&
                  y.prefix.len() <= 20 &&
                  (min_string_len(&x.suffix) + min_string_len(&y.prefix)) >= 3 {
        and_trigrams(xy.query, &cross_product(&x.suffix, &y.prefix))
    } else {
        x.query.and(y.query)
    };

    if let (&Some(ref x_s), &Some(ref y_s)) = (&x.exact_set, &y.exact_set) {
        xy.exact_set = Some(cross_product(&x_s, &y_s));
    } else {
        if let &Some(ref x_s) = &x.exact_set {
            xy.prefix = cross_product(&x_s, &y.prefix);
        } else {
            xy.prefix = if x.can_empty {
                x.prefix
            } else {
                union(&x.prefix, &y.prefix)
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

    xy
}

/// Returns the RegexInfo for x|y given x and y
fn alternate(x: RegexInfo, y: RegexInfo) -> RegexInfo {
    let mut x = x;
    let mut y = y;
    let mut xy = RegexInfo::default();
    match (&x.exact_set, &y.exact_set) {
        (&Some(ref x_s), &Some(ref y_s)) => {
            xy.exact_set = Some(union(&x_s, y_s));
        }
        (&Some(ref x_s), &None) => {
            xy.prefix = union(&x_s, &y.prefix);
            xy.suffix = union(&x_s, &y.suffix);
            x.query = and_trigrams(x.query, x_s);
        }
        (&None, &Some(ref y_s)) => {
            xy.prefix = union(&x.prefix, &y_s);
            xy.suffix = union(&x.suffix, &y_s);
            y.query = and_trigrams(y.query, y_s);
        }
        _ => {
            xy.prefix = union(&x.prefix, &y.prefix);
            xy.suffix = union(&x.suffix, &y.suffix);
        }
    }
    xy.can_empty = x.can_empty || y.can_empty;
    xy.query = x.query.or(y.query);
    xy
}

/// Returns the length of the shortest string in xs
fn min_string_len(xs: &HashSet<String>) -> usize {
    xs.iter().map(String::len).min().unwrap()
}

/// Returns the cross product of s and t
fn cross_product(s: &HashSet<Trigram>, t: &HashSet<Trigram>) -> HashSet<Trigram> {
    let mut p = HashSet::new();
    for s_string in s {
        for t_string in t {
            let mut cross_string = s_string.clone();
            cross_string.push_str(&t_string);
            p.insert(cross_string);
        }
    }
    p
}

fn trigrams_imply(trigram: &HashSet<Trigram>, rhs: &Query) -> bool {
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

fn and_trigrams(q: Query, t: &HashSet<Trigram>) -> Query {
    if min_string_len(t) < 3 {
        // If there is a short string, we can't guarantee
        // that any trigrams must be present, so use ALL.
        // q AND ALL = q.
        return q;
    }
    let or = t.iter().fold(Query::none(), |or, t_string| {
        let mut trigram = HashSet::<Trigram>::new();
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
        if operation == QueryOperation::And {
            return q;
        } else {
            return r;
        }
    }
    if r.implies(&q) {
        if operation == QueryOperation::And {
            return r;
        } else {
            return q;
        }
    }
    // Both q and r are QAnd or QOr.
    // If they match or can be made to match, merge.
    if q.operation == operation && (r.operation == operation || r.is_atom()) {
        q.trigram = union(&q.trigram, &r.trigram);
        q.sub.append(&mut r.sub);
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
            trigram: HashSet::new(),
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

fn union<T: Eq + Hash + Clone>(s: &HashSet<T>, t: &HashSet<T>) -> HashSet<T> {
    s.union(t).cloned().collect()
}

fn intersection<T: Eq + Hash + Clone>(s: &HashSet<T>, t: &HashSet<T>) -> HashSet<T> {
    s.intersection(t).cloned().collect()
}

fn difference<T: Eq + Hash + Clone>(s: &HashSet<T>, t: &HashSet<T>) -> HashSet<T> {
    s.difference(t).cloned().collect()
}
