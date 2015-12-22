/// Copy of go's sort.Search function
use num::traits::{Num, FromPrimitive};

pub fn search<I, F>(n: I, f: F) -> I
    where I: Copy + Num + FromPrimitive + PartialOrd,
          F: Fn(I) -> bool
{
    let mut i: I = I::zero();
    let mut j: I = n;
    while i < j {
        let h = i + (j-i) / I::from_i64(2).unwrap();
        if !f(h) {
            i = h + I::one();
        } else {
            j = h;
        }
    }
    i
}

#[test]
fn test_middle() {
    let value = search(20, |i| {
        i > 10
    });
    assert!(value == 11);
}
