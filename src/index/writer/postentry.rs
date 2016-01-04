#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
pub struct PostEntry(pub u64);

impl PostEntry {
    pub fn new(trigram: u32, file_id: u32) -> Self {
        PostEntry((trigram as u64) << 32 | (file_id as u64))
    }
    pub fn trigram(&self) -> u32 {
        let &PostEntry(ref u) = self;
        return (u >> 32) as u32;
    }
    pub fn file_id(&self) -> u32 {
        let &PostEntry(ref u) = self;
        return (u & 0xffffffff) as u32;
    }
    pub fn value(&self) -> u64 {
        let &PostEntry(v) = self;
        v
    }
}


