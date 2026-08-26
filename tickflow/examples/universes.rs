use std::env;

use tickflow::client::TickFlow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("TICKFLOW_API_KEY").expect("TICKFLOW_API_KEY must be set");
    let tf = TickFlow::new(api_key)?;

    // 列出所有 Universe
    let universes = tf.universes().list().await?;
    for u in &universes.data {
        println!("{}: {} ({}, {})", u.id, u.name, u.region, u.category);
    }

    // 单 Universe 详情
    let u = tf.universes().get("CN_Equity_A").send().await?;
    println!("{} 共 {} 个标的", u.id, u.symbol_count.unwrap_or(0));
    if let Some(symbols) = &u.symbols {
        println!("前 5: {:?}", &symbols[..symbols.len().min(5)]);
    }

    // 批量 Universe
    let universes = tf
        .universes()
        .batch(["CN_Equity_A", "US_Equity", "HK_Equity"])
        .send()
        .await?;
    for u in universes {
        println!("{}: {}", u.id, u.name);
    }

    Ok(())
}
