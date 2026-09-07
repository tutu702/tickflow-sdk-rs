use std::env;

use tickflow::client::TickFlow;
use tickflow::model::{BatchIntradayResponse, IntradayResponse, Period};
use tickflow::resources::klines::IntradayBuilderExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("TICKFLOW_API_KEY").expect("TICKFLOW_API_KEY must be set");
    let tf = TickFlow::new(api_key)?;

    // 单标的分时
    let intraday = match tf
        .klines()
        .intraday("600000.SH")
        .period(Period::M1) // 默认 1m
        .count(240) // A 股一天约 240 根 1m K 线
        .send()
        .await?
    {
        IntradayResponse::Raw(d) => d,
        IntradayResponse::DataFrame(_) => unreachable!(),
    };
    println!(
        "分时 {} 根，最后价 {:?}",
        intraday.len(),
        intraday.last_close()
    );

    // 批量分时
    let intraday = match tf
        .klines()
        .intraday_batch(["600000.SH", "000001.SZ"])
        .period(Period::M5)
        .count(48)
        .send()
        .await?
    {
        BatchIntradayResponse::Raw(m) => m,
        BatchIntradayResponse::DataFrame(_) => unreachable!(),
    };
    for (symbol, kl) in &intraday {
        println!("{symbol}: {} 根", kl.len());
    }

    Ok(())
}
