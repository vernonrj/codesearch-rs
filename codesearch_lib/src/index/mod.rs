pub mod read;
pub mod write;
pub mod regexp;
pub mod merge;
mod search;

use std::env;

pub const MAGIC: &'static str        = "csearch index 1\n";
pub const TRAILER_MAGIC: &'static str = "\ncsearch trailr\n";

pub fn csearch_index() -> String {
    env::var("CSEARCHINDEX")
        .or_else(|_| env::var("HOME").or_else(|_| env::var("USERPROFILE"))
                        .map(|s| s + &"/.csearchindex"))
        .expect("no valid path to index")
}
