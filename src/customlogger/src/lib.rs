/// custom logger
extern crate chrono;
extern crate log;

use chrono::Local;
use log::{Log, LogRecord, LogLevelFilter, LogMetadata, SetLoggerError};

pub struct Logger {
    max_level: LogLevelFilter
}

impl Log for Logger {
    fn enabled(&self, metadata: &LogMetadata) -> bool {
        metadata.level() <= self.max_level
    }
    fn log(&self, record: &LogRecord) {
        if self.enabled(record.metadata()) {
            let now = Local::now();
            let now_time = now.format("%Y/%m/%d %H:%M:%S");
            println!("{} {}", now_time, record.args());
        }
    }
}

pub fn init(level: LogLevelFilter) -> Result<(), SetLoggerError> {
    log::set_logger(|max_log_level| {
        max_log_level.set(level);
        Box::new(Logger { max_level: level })
    })
}

