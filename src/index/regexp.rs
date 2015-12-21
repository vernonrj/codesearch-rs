/// Regular Expression matching
use std::collections::HashSet;

// use regex::Regex;
use regex_syntax::{Expr, Repeater};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum QueryOperation {
    All,
    None,
    And,
    Or
}


#[derive(Debug)]
pub struct Query {
    operation: QueryOperation,
    trigram: HashSet<String>,
    sub: Vec<Query>
}

impl Default for Query {
    fn default() -> Self {
        Query {
            operation: QueryOperation::All,
            trigram: HashSet::new(),
            sub: Vec::new()
        }
    }
}

impl Query {
    pub fn new(operation: QueryOperation) -> Query {
        Query {
            operation: operation,
            trigram: HashSet::new(),
            sub: Vec::new()
        }
    }
    // pub fn from_regex(expr: Regex) -> Query {
    //     RegexInfo::new(expr)
    //         .simplify(true)
    //         .add_exact()
    //         .query
    // }
    pub fn all() -> Query { Query::new(QueryOperation::All) }
    pub fn none() -> Query { Query::new(QueryOperation::None) }
    pub fn implies(&self, rhs: &Query) -> bool {
        match (self.operation, rhs.operation) {
            (QueryOperation::None, _) | (_, QueryOperation::All) => {
                // False implies everything.
                // Everything implies True.
                return true;
            },
            (QueryOperation::All, _) | (_, QueryOperation::None) => {
                // True implies nothing.
                // Nothing implies False.
                return false;
            },
            (_, _) => ()
        }
        if self.operation == QueryOperation::And
            || (self.operation == QueryOperation::Or
                && self.trigram.len() == 1
                && self.sub.len() == 0)
        {
            return trigrams_imply(&self.trigram, rhs);
        }
        if self.operation == QueryOperation::Or && rhs.operation == QueryOperation::Or
            && self.trigram.len() > 0 && self.sub.len() == 0
            && self.trigram.is_subset(&rhs.trigram)
        {
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
}

#[derive(Default)]
pub struct RegexInfo {
    pub can_empty: bool,
    pub exact_set: Option<HashSet<String>>,
    pub prefix: HashSet<String>,
    pub suffix: HashSet<String>,
    pub query: Query
}

impl RegexInfo {
    pub fn new(expr: &Expr) -> Self {
        match expr {
            &Expr::Empty
            | &Expr::StartLine | &Expr::EndLine
            | &Expr::StartText | &Expr::EndText
            | &Expr::WordBoundary | &Expr::NotWordBoundary => {
                return Self::empty_string();
            },
            &Expr::Literal {ref chars, ref casei} => {
                if *casei == true {
                    match chars.len() {
                        0 => return Self::empty_string(),
                        1 => { 
                            unimplemented!();
                        },
                        _ => ()
                    }
                    // Multi-letter case-folded string:
                    // treat as concatenation of single-letter case-folded strings.
                    let mut info = Self::empty_string();
                    for i in 0 .. chars.len() {
                        let rune = vec![chars[i]];
                        let re1 = Expr::Literal {
                            chars: rune,
                            casei: *casei
                        };
                        info = concat(info, Self::new(&re1))
                    }
                    return info;
                }
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
                    query: and_trigrams(Query::all(), &exact_set)
                }
            },
            &Expr::AnyChar | &Expr::AnyCharNoNL => {
                return Self::any_char();
            },
            &Expr::Concat(ref v) => {
                let analyzed = v.iter().map(RegexInfo::new);
                return analyzed.fold(Self::empty_string(), concat);
            },
            &Expr::Alternate(ref v) => {
                let analyzed = v.iter().map(RegexInfo::new);
                return analyzed.fold(Self::no_match(), alternate);
            },
            &Expr::Repeat {ref e, ref r, /* ref greedy */ .. } => {
                match r {
                    &Repeater::ZeroOrOne => {
                        return alternate(RegexInfo::new(e), Self::empty_string());
                    },
                    &Repeater::ZeroOrMore => {
                        // We don't know anything, so assume the worst.
                        return Self::any_match();
                    },
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
                        return info;
                    },
                    &Repeater::Range {..} => unimplemented!() /* is this needed? */
                }
            },
            &Expr::Class(_) => {
                // let mut info = RegexInfo {
                //     can_empty: false,
                //     exact_set: None,
                //     prefix: HashSet::new(),
                //     suffix: HashSet::new(),
                //     query: Query::all()
                // };
                unimplemented!(); // Don't know what to do from here...
            },
            _ => unimplemented!() /* Still have more cases to implement */
        }
    }
    fn no_match() -> Self {
        RegexInfo {
            can_empty: false,
            exact_set: None,
            prefix: HashSet::new(),
            suffix: HashSet::new(),
            query: Query::new(QueryOperation::None)
        }
    }
    fn empty_string() -> Self {
        let mut exact_set = HashSet::new();
        exact_set.insert("".to_string());
        RegexInfo {
            can_empty: true,
            exact_set: Some(exact_set),
            prefix: HashSet::new(),
            suffix: HashSet::new(),
            query: Query::all()
        }
    }
    fn any_char() -> Self {
        RegexInfo {
            can_empty: false,
            exact_set: None,
            prefix: {
                let mut p = HashSet::new();
                p.insert("".to_string());
                p
            },
            suffix: {
                let mut s = HashSet::new();
                s.insert("".to_string());
                s
            },
            query: Query::all()
        }
    }
    fn any_match() -> Self {
        RegexInfo {
            can_empty: true,
            exact_set: None,
            prefix: {
                let mut h = HashSet::new();
                h.insert("".to_string());
                h
            },
            suffix: {
                let mut h = HashSet::new();
                h.insert("".to_string());
                h
            },
            query: Query::new(QueryOperation::All)
        }
    }
}

fn concat(x: RegexInfo, y: RegexInfo) -> RegexInfo {
    let mut xy = RegexInfo::default();
    xy.query = x.query.and(y.query);
    if let (&Some(ref x_s), &Some(ref y_s)) = (&x.exact_set, &y.exact_set) {
        xy.exact_set = Some(cross_product(&x_s, &y_s));
    } else {
        if let &Some(ref x_s) = &x.exact_set {
            xy.prefix = cross_product(&x_s, &y.prefix);
        } else {
            xy.prefix = if x.can_empty {
                x.prefix
            } else {
                x.prefix.union(&y.prefix).cloned().collect()
            };
        }
        if let &Some(ref y_s) = &y.exact_set {
            xy.suffix = cross_product(&x.suffix, &y_s);
        } else {
            xy.suffix = if y.can_empty {
                y.suffix.union(&x.suffix).cloned().collect()
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
    if x.exact_set.is_none() && y.exact_set.is_none()
        && x.suffix.len() <= 20 && y.prefix.len() <= 20
        && (min_string_len(&x.suffix) + min_string_len(&y.prefix)) >= 3
    {
        xy.query = and_trigrams(xy.query, &cross_product(&x.suffix, &y.prefix));
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
            xy.exact_set = Some(x_s.union(y_s).cloned().collect());
        },
        (&Some(ref x_s), &None) => {
            xy.prefix = x_s.union(&y.prefix).cloned().collect();
            xy.suffix = x_s.union(&y.suffix).cloned().collect();
            x.query = and_trigrams(x.query, x_s);
        },
        (&None, &Some(ref y_s)) => {
            xy.prefix = x.prefix.union(&y_s).cloned().collect();
            xy.suffix = x.suffix.union(&y_s).cloned().collect();
            y.query = and_trigrams(y.query, y_s);
        },
        _ => {
            xy.prefix = x.prefix.union(&y.prefix).cloned().collect();
            xy.suffix = x.suffix.union(&y.suffix).cloned().collect();
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
fn cross_product(s: &HashSet<String>, t: &HashSet<String>) -> HashSet<String> {
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

fn is_subset_of(lhs: &Vec<String>, rhs: &Vec<String>) -> bool {
    lhs.iter().all(|s| rhs.contains(s))
}

fn trigrams_imply(trigram: &HashSet<String>, rhs: &Query) -> bool {
    match rhs.operation {
        QueryOperation::Or => {
            if rhs.sub.iter().any(|s| trigrams_imply(trigram, s)) {
                return true;
            }
            if trigram.iter().any(|s| rhs.trigram.contains(s)) {
                return true;
            }
            return false;
        },
        QueryOperation::And => {
            if !rhs.sub.iter().any(|s| trigrams_imply(trigram, s)) {
                return false;
            }
            if !trigram.iter().any(|s| rhs.trigram.contains(s)) {
                return false;
            }
            return true;
        },
        _ => false
    }
}

fn and_trigrams(q: Query, t: &HashSet<String>) -> Query {
    if min_string_len(t) < 3 {
        // If there is a short string, we can't guarantee
        // that any trigrams must be present, so use ALL.
        // q AND ALL = q.
        return q;
    }
    let mut or = Query::none();
    for t_string in t {
        let mut trigram = HashSet::<String>::new();
        for i in 0 .. (t_string.len() - 2) {
            trigram.insert(t_string[i .. i + 3].to_string());
        }
        or = or.or(Query {
            operation: QueryOperation::And,
            trigram: trigram,
            sub: Vec::new()
        });
    }
    q.and(or)
}

/**
 * returns self OP other, possibly reusing self and other's storage.
 */
fn and_or(mut q: Query, mut r: Query, operation: QueryOperation) -> Query {
    let mut q = if q.trigram.len() == 0 && q.sub.len() == 1 {
        q.sub.pop().unwrap()
    } else {
        q
    };
    let mut r = if r.trigram.len() == 0 && r.sub.len() == 1 {
        r.sub.pop().unwrap()
    } else {
        r
    };

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
    let is_q_atom = q.trigram.len() == 1 && q.sub.len() == 0;
    let is_r_atom = r.trigram.len() == 1 && r.sub.len() == 0;
    if q.operation == operation && (r.operation == operation || is_r_atom) {
        q.trigram = q.trigram.union(&r.trigram).cloned().collect();
        q.sub.append(&mut r.sub);
        return q;
    }
    if r.operation == operation && is_q_atom {
        r.trigram = r.trigram.union(&q.trigram).cloned().collect();
        return r;
    }
    if is_q_atom && is_r_atom {
        q.operation = operation;
        q.trigram = q.trigram.union(&r.trigram).cloned().collect();
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
    let common = q.trigram.intersection(&r.trigram).cloned().collect();
    q.trigram = q.trigram.difference(&common).cloned().collect();
    r.trigram = r.trigram.difference(&common).cloned().collect();
    if common.len() > 0 {
		// If there were common trigrams, rewrite
		//
		//	(abc|def|ghi|jkl) AND (abc|def|mno|prs) =>
		//		(abc|def) OR ((ghi|jkl) AND (mno|prs))
		//
		//	(abc&def&ghi&jkl) OR (abc&def&mno&prs) =>
		//		(abc&def) AND ((ghi&jkl) OR (mno&prs))
		//
		// Build up the right one of
		//	(ghi|jkl) AND (mno|prs)
		//	(ghi&jkl) OR (mno&prs)
		// Call andOr recursively in case q and r can now be simplified
		// (we removed some trigrams).
        let s = and_or(q, r, operation);
        let new_operation = match operation {
            QueryOperation::And => QueryOperation::Or,
            QueryOperation::Or => QueryOperation::And,
            _ => panic!("unexpected query operation: {:?}", operation)
        };
        let t = Query {
            operation: new_operation,
            trigram: common,
            sub: Vec::new()
        };
        return and_or(t, s, new_operation);
    }

	// Otherwise just create the op.
    Query {
        operation: operation,
        trigram: HashSet::new(),
        sub: vec![q, r]
    }
}
