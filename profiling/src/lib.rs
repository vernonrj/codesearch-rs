

#[cfg(feature = "profile")]
#[macro_use]
mod profiling {
    extern crate hprof;
    #[allow(dead_code)]
    pub fn profile(name: &'static str) -> self::hprof::ProfileGuard<'static> {
        self::hprof::enter(name)
    }
    #[allow(dead_code)]
    pub fn print_profiling() {
        self::hprof::profiler().print_timing();
    }
}
#[cfg(not(feature = "profile"))]
#[macro_use]
mod profiling {
    #[allow(dead_code)]
    pub fn profile(_: &'static str) -> () {
        ()
    }
    #[allow(dead_code)]
    pub fn print_profiling() {
        // no-op
    }
}

pub use self::profiling::*;
