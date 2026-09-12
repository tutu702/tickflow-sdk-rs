use crate::error::{Error, Result};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub mod depth;
pub mod exchanges;
pub mod financials;
pub mod instruments;
pub mod klines;
pub mod quotes;
pub mod universes;

pub use depth::Depth;
pub use exchanges::Exchanges;
pub use financials::Financials;
pub use instruments::Instruments;
pub use klines::Klines;
pub use quotes::Quotes;
pub use universes::Universes;

pub(crate) const BATCH_CONCURRENCY: usize = 5;
pub(crate) const BATCH_CHUNK_SIZE: usize = 500;

/// Envelope shared by every batch endpoint: `{"data": {symbol: payload}}`.
#[derive(Deserialize)]
pub(crate) struct DataResponse<T> {
    pub(crate) data: HashMap<String, T>,
}

/// Convert a `symbol -> records` map into a long-format polars [`DataFrame`].
fn records_to_dataframe<T: Serialize>(map: &HashMap<String, Vec<T>>) -> Result<DataFrame> {
    let mut symbols: Vec<&str> = Vec::new();
    let mut columns: Vec<(String, Vec<serde_json::Value>)> = Vec::new();
    let mut index: HashMap<String, usize> = HashMap::new();

    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort_unstable();

    for symbol in keys {
        for record in &map[symbol] {
            let value = serde_json::to_value(record)
                .map_err(|e| Error::DataFrame(format!("cannot serialize record: {e}")))?;
            let serde_json::Value::Object(fields) = value else {
                return Err(Error::DataFrame(
                    "financial record did not serialize to a JSON object".to_owned(),
                ));
            };

            let row = symbols.len();
            symbols.push(symbol.as_str());

            for (name, field) in fields {
                let col = match index.get(&name) {
                    Some(&i) => i,
                    None => {
                        index.insert(name.clone(), columns.len());
                        columns.push((name, Vec::new()));
                        columns.len() - 1
                    }
                };
                // Pad columns introduced by a later record (or skipped by
                // `skip_serializing_if`) so every column stays row-aligned.
                columns[col].1.resize(row, serde_json::Value::Null);
                columns[col].1.push(field);
            }
        }
    }

    let height = symbols.len();
    let mut out: Vec<Column> = Vec::with_capacity(columns.len() + 1);
    out.push(Column::new("symbol".into(), symbols));
    for (name, mut values) in columns {
        values.resize(height, serde_json::Value::Null);
        out.push(json_column(&name, &values));
    }

    DataFrame::new(out).map_err(|e| Error::DataFrame(e.to_string()))
}

/// Build one typed polars column from a column of JSON values.
///
/// The dtype comes from the first non-null value; values that do not match it
/// are emitted as nulls rather than failing the whole frame.
fn json_column(name: &str, values: &[serde_json::Value]) -> Column {
    let first = values.iter().find(|v| !v.is_null());
    match first {
        Some(serde_json::Value::String(_)) => {
            let data: Vec<Option<&str>> = values.iter().map(serde_json::Value::as_str).collect();
            Column::new(name.into(), data)
        }
        Some(serde_json::Value::Bool(_)) => {
            let data: Vec<Option<bool>> = values.iter().map(serde_json::Value::as_bool).collect();
            Column::new(name.into(), data)
        }
        Some(serde_json::Value::Number(_)) => {
            let data: Vec<Option<f64>> = values.iter().map(serde_json::Value::as_f64).collect();
            Column::new(name.into(), data)
        }
        // All-null column, or a nested array/object we cannot flatten: fall
        // back to the JSON text so no data is silently dropped.
        _ => {
            let data: Vec<Option<String>> = values
                .iter()
                .map(|v| {
                    if v.is_null() {
                        None
                    } else {
                        Some(v.to_string())
                    }
                })
                .collect();
            Column::new(name.into(), data)
        }
    }
}
