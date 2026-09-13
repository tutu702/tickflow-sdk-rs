//! Rust SDK for the TickFlow financial market-data API.
//!
//! TickFlow exposes endpoints for quotes, klines, intraday bars, market
//! depth, instrument metadata, universes, exchange listings, and
//! financial statements (income / balance-sheet / cash-flow / metrics /
//! shares).
//!
//! # Quick start
//!
//! ```no_run
//! use tickflow_sdk_rs::TickFlow;
//!
//! # async fn run() -> Result<(), tickflow_sdk_rs::error::Error> {
//! let client = TickFlow::new("API_KEY")?;
//! let raw = client.quotes().symbol("AAPL.US").send().await?;
//! # let _ = raw;
//! # Ok(()) }
//! ```
//!
//! Most resources expose both a "raw" mode (returning the underlying
//! `serde`-deserialised DTO) and a DataFrame mode (selectable on the
//! builder). Construct a client with
//! [`TickFlow::new`](client::TickFlow::new), then call the resource
//! accessor you need:
//!
//! - [`TickFlow::quotes`](client::TickFlow::quotes) — last trade /
//!   top-of-book snapshot
//! - [`TickFlow::klines`](client::TickFlow::klines) — daily + intraday
//!   OHLCV bars
//! - [`TickFlow::instruments`](client::TickFlow::instruments) /
//!   [`TickFlow::exchanges`](client::TickFlow::exchanges) — instrument
//!   and exchange metadata
//! - [`TickFlow::universes`](client::TickFlow::universes) — curated
//!   symbol collections
//! - [`TickFlow::depth`](client::TickFlow::depth) — order-book
//!   snapshots
//! - [`TickFlow::financials`](client::TickFlow::financials) — fundamental
//!   statements
//!
//! See the [`client`] module for builder-level configuration
//! (custom `base_url`, timeouts, the free-tier endpoint, etc.).

pub mod cache;
pub mod client;
pub mod error;
pub mod http;
pub mod model;
pub mod resources;
