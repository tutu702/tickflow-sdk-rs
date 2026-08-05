pub mod depth;
pub mod exchanges;
pub mod instruments;
pub mod klines;
pub mod quotes;
pub mod universes;

pub use depth::Depth;
pub use exchanges::Exchanges;
pub use instruments::Instruments;
pub use klines::Klines;
pub use quotes::Quotes;
pub use universes::Universes;

pub(crate) const BATCH_CONCURRENCY: usize = 5;
pub(crate) const BATCH_CHUNK_SIZE: usize = 500;
