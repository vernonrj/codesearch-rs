// Copyright 2015 Vernon Jones.
// Original code Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
use std::char;
use std::collections::BTreeSet;
use std::iter::FromIterator;

pub type StringSet = BTreeSet<Vec<u8>>;

// use regex::Regex;
use regex_syntax::{Expr, Repeater, ByteClass, ClassRange, ByteRange};

/// Operation on a Query
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum QueryOperation {
    All,
    None,
    And,
    Or,
}

pub type Trigram = Vec<u8>;

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
            let tri = self.trigram.iter().next().unwrap();
            s.push_str(&format!("\"{}\"", String::from_utf8_lossy(&tri[..])));
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
            s.push_str(&format!("\"{}\"", String::from_utf8_lossy(&t[..])));
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
    pub fn new(expr: Expr) -> Result<Self, String> {
        let mut info = Self::analyze(expr)?;
        info = simplify(info, true);
        add_exact(&mut info);
        Ok(info)
    }
    fn analyze(expr: Expr) -> Result<Self, String> {
        // println!("expr: {:?}", expr);
        match expr {
            Expr::Empty |
            Expr::StartLine |
            Expr::EndLine |
            Expr::StartText |
            Expr::EndText |
            Expr::WordBoundary |
            Expr::NotWordBoundary => Ok(Self::empty_string()),
            Expr::WordBoundaryAscii |
            Expr::NotWordBoundaryAscii => Ok(Self::empty_string()),
            Expr::Literal {chars, casei } => {
                Self::analyze(Expr::LiteralBytes {
                    bytes: String::from_iter(chars.into_iter()).into_bytes(),
                    casei: casei
                })
            }
            Expr::LiteralBytes { bytes, casei: true } => {
                match bytes.len() {
                    0 => Ok(Self::empty_string()),
                    1 => {
                        let re1 = Expr::ClassBytes(ByteClass::new(vec![ByteRange {
                                                                      start: bytes[0],
                                                                      end: bytes[0],
                                                                  }]).case_fold());
                        Self::analyze(re1)
                    }
                    _ => {
                        // Multi-letter case-folded string:
                        // treat as concatenation of single-letter case-folded strings.
                        let folded = bytes
                            .into_iter()
                            .fold(Ok(Self::empty_string()), |info, c| {
                                let analyzed = try!(Self::analyze(Expr::LiteralBytes {
                                    bytes: vec![c],
                                    casei: true
                                }));
                                info.map(|info| concat(info, analyzed))
                            });
                        folded
                    }
                }
            }
            Expr::LiteralBytes { bytes, casei: false} => {
                let exact_set = {
                    let mut h = StringSet::new();
                    h.insert(bytes);
                    h
                };
                let r = RegexInfo {
                    can_empty: false,
                    exact_set: Some(exact_set.clone()),
                    prefix: StringSet::new(),
                    suffix: StringSet::new(),
                    query: Query::all()
                };
                Ok(simplify(r, false))
            }
            Expr::AnyChar | Expr::AnyCharNoNL => Ok(Self::any_char()),
            Expr::AnyByte | Expr::AnyByteNoNL => Ok(Self::any_char()),
            Expr::Concat(exprs) => {
                let mut exprs = exprs.into_iter().map(Self::analyze);
                let first = match exprs.next() {
                    Some(ex) => ex,
                    None => return Ok(Self::empty_string()),
                };
                exprs.fold(first, |a, b| {
                    match (a, b) {
                        (Ok(a), Ok(b)) => Ok(concat(a, b)),
                        (Err(e), _) | (_, Err(e)) => Err(e)
                    }
                })
            }
            Expr::Alternate(v) => {
                let mut v = v.into_iter().map(Self::analyze);
                let first = match v.next() {
                    Some(f) => f,
                    None => return Ok(Self::no_match())
                };
                v.fold(first, |a, b| {
                    match (a, b) {
                        (Ok(a), Ok(b)) => Ok(alternate(a, b)),
                        (Err(e), _) | (_, Err(e)) => Err(e)
                    }
                })
            }
            Expr::Repeat {e, r, /* ref greedy */ .. } => {
                match r {
                    Repeater::ZeroOrOne => {
                        let e = Self::analyze(*e)?;
                        Ok(alternate(e, Self::empty_string()))
                    },
                    Repeater::ZeroOrMore | Repeater::Range {..} => {
                        // We don't know anything, so assume the worst.
                        Ok(Self::any_match())
                    },
                    Repeater::OneOrMore => {
                        // x+
                        // Since there has to be at least one x, the prefixes and suffixes
                        // stay the same.  If x was exact, it isn't anymore.

                        let mut info = Self::analyze(*e)?;
                        if let Some(i_s) = info.exact_set {
                            info.prefix = i_s.clone();
                            info.suffix = i_s;
                            info.exact_set = None;
                        }
                        Ok(simplify(info, false))
                    },
                }
            }
            Expr::Class(ref ranges) if ranges.is_empty() => Ok(Self::no_match()),
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
                    match (end as u32).checked_sub(start as u32) {
                        Some(x) if x > 100 => return Ok(Self::any_char()),
                        Some(_) => (),
                        None => {
                            return Err(format!("range not in ascending order ({}..{})",
                                               start, end));
                        }
                    }
                    let next_range: StringSet = {
                        let mut h = StringSet::new();
                        for chr in CharRangeIter::new(start, end)? {
                            let mut s = String::new();
                            s.push(chr);
                            h.insert(s.into_bytes());
                        }
                        h
                    };
                    if let Some(ref mut exact) = info.exact_set {
                        *exact = union(&exact, &next_range);
                    } else {
                        info.exact_set = Some(next_range);
                    }
                }
                Ok(simplify(info, false))
            },
            Expr::ClassBytes(ref ranges) if ranges.is_empty() => Ok(Self::no_match()),
            Expr::ClassBytes(ref ranges) => {
                let mut info = RegexInfo {
                    can_empty: false,
                    exact_set: None,
                    prefix: StringSet::new(),
                    suffix: StringSet::new(),
                    query: Query::all(),
                };
                for each_range in ranges {
                    let &ByteRange { start, end } = each_range;
                    // if the class is too large, it's okay to overestimate
                    match end.checked_sub(start) {
                        Some(x) if x > 100 => return Ok(Self::any_char()),
                        Some(_) => (),
                        None => {
                            return Err(format!("range not in ascending order ({}..{})",
                                               start, end));
                        }
                    }
                    let next_range: StringSet = {
                        let mut h = StringSet::new();
                        for chr in start..end+1 {
                            h.insert(vec![chr]);
                        }
                        h
                    };
                    if let Some(ref mut exact) = info.exact_set {
                        *exact = union(&exact, &next_range);
                    } else {
                        info.exact_set = Some(next_range);
                    }
                }
                Ok(simplify(info, false))
            },
            Expr::Group { e, .. } => {
                Self::analyze(*e)
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
        h.insert(Vec::new());
        h
    }
    pub fn format_as_string(&self) -> String {
        let mut s = String::new();
        if self.can_empty {
            s.push_str("canempty ");
        }
        if let Some(ref exact) = self.exact_set {
            s.push_str("exact: ");
            let as_vec: Vec<&[u8]> = exact.iter().map(|v| v as &[u8]).collect();
            let flattened: Vec<u8> = as_vec.join(&b',');
            s.push_str(&*String::from_utf8_lossy(&flattened));
        } else {
            s.push_str("prefix: ");
            let as_vec: Vec<&[u8]> = self.prefix.iter().map(|v| v as &[u8]).collect();
            let flattened: Vec<u8> = as_vec.join(&b',');
            s.push_str(&*String::from_utf8_lossy(&flattened));
            s.push_str(" suffix: ");
            let as_vec: Vec<&[u8]> = self.suffix.iter().map(|v| v as &[u8]).collect();
            let flattened: Vec<u8> = as_vec.join(&b',');
            s.push_str(&*String::from_utf8_lossy(&flattened));
        }
        s.push_str(&format!(" match: {}", self.query.format_as_string()));
        s
    }
}

fn concat(x: RegexInfo, y: RegexInfo) -> RegexInfo {
    let mut xy = RegexInfo::default();

    xy.query = x.query.and(y.query);

    if let (&Some(ref x_s), &Some(ref y_s)) = (&x.exact_set, &y.exact_set) {
        xy.exact_set = Some(cross_product(x_s, y_s));
    } else {
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
    if x.exact_set.is_none() && y.exact_set.is_none() && x.suffix.len() <= 20 &&
                  y.prefix.len() <= 20 &&
                  (min_string_len(&x.suffix) + min_string_len(&y.prefix)) >= 3 {
        xy.query = and_trigrams(xy.query, &cross_product(&x.suffix, &y.prefix))
    }

    xy = simplify(xy, false);
    xy
}

/// Returns the RegexInfo for x|y given x and y
fn alternate(x: RegexInfo, y: RegexInfo) -> RegexInfo {
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
    simplify(xy, false)
}

fn add_exact(x: &mut RegexInfo) {
    let exact = if let Some(ref exact) = x.exact_set {
        exact.clone()
    } else {
        return;
    };
    x.query = and_trigrams(x.query.clone(), &exact);
}

fn simplify(mut info: RegexInfo, force: bool) -> RegexInfo {
    // println!("simplify {:?}: {}", info, info.format_as_string());
    let do_simplify = if let Some(ref exact) = info.exact_set {
        exact.len() > 7 
            || (min_string_len(&exact) >= 3 && force)
            || min_string_len(&exact) >= 4
    } else {
        false
    };

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
                    let first_three_chars = s.iter().take(2).cloned().collect();
                    let rest = s.iter().skip(n-2).cloned().collect();
                    info.prefix.insert(first_three_chars);
                    info.suffix.insert(rest);
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
    q = and_trigrams(q, prefix_or_suffix);
    let mut n = 3;
    while n == 3 || prefix_or_suffix.len() > 20 {
        let mut t = StringSet::new();
        for string in prefix_or_suffix.iter() {
            let mut s = string.clone();
            if s.len() >= n {
                s = if !is_suffix {
                    s.iter().take(n-1).cloned().collect()
                } else {
                    s.iter().skip(s.len()-n+1).cloned().collect()
                };
            }
            t.insert(s);
        }
        *prefix_or_suffix = t;
        n -= 1;
    }
    // Now make sure that the prefix/suffix sets aren't redundant.
    // For example, if we know "ab" is a possible prefix, then it
    // doesn't help at all to know that  "abc" is also a possible
    // prefix, so delete "abc".

    let f = |a: &[u8], b: &[u8]| {
        if is_suffix {
            a.starts_with(b)
        } else {
            a.ends_with(b)
        }
    };
    let mut u = StringSet::new();
    let mut last: Option<Vec<u8>> = None;
    for s in prefix_or_suffix.iter() {
        if u.is_empty() || !f(&s, &last.as_ref().unwrap()) {
            u.insert(s.clone());
            last = Some(s.clone());
        }
    }

    *prefix_or_suffix = u;
    q
}

/// Returns the length of the shortest string in xs
fn min_string_len(xs: &StringSet) -> usize {
    xs.iter().map(Vec::len).min().unwrap_or(0)
}

/// Returns the cross product of s and t
fn cross_product(s: &StringSet, t: &StringSet) -> StringSet {
    let mut p = StringSet::new();
    for s_tri in s {
        for t_tri in t {
            let mut cross_string = s_tri.clone();
            cross_string.extend(t_tri);
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
            if rhs.sub.iter().any(|s| !trigrams_imply(trigram, s)) {
                return false;
            }
            if !rhs.trigram.is_subset(trigram) {
                return false;
            }
            return true;
        }
        _ => false,
    }
}

fn and_trigrams(q: Query, t: &BTreeSet<Trigram>) -> Query {
    if min_string_len(t) < 3 {
        // If there is a short string, we can't guarantee
        // that any trigrams must be present, so use ALL.
        // q AND ALL = q.
        return q;
    }
    let or = t.iter().fold(Query::none(), |or, t_string| {
        let mut trigram = BTreeSet::<Trigram>::new();
        // NOTE: the .windows() slice method would be better here,
        //       but it doesn't seem to be available for chars
        for i in 0..(t_string.len() - 2) {
            trigram.insert(Vec::from(&t_string[i..i + 3]));
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
    fn new(low: char, high: char) -> Result<Self, String> {
        if (low as u32) > (high as u32) {
            Err(format!("start ({}) > end ({})", low, high))
        } else if low > char::MAX || high > char::MAX {
            Err(format!("low or high > char::MAX"))
        } else {
            Ok(CharRangeIter {
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
