pub mod klines;
pub mod quotes;

pub use klines::Klines;
pub use quotes::Quotes;

pub(crate) const BATCH_CONCURRENCY: usize = 5;
