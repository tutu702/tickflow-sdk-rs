use std::env;
use tickflow::client::TickFlow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("TICKFLOW_API_KEY").expect("TICKFLOW_API_KEY must be set");

    let tf = TickFlow::new(&api_key)?;

    // 1) 单标的（GET）
    let mut quotes = tf.quotes().symbol("600000.SH").send().await?;
    let q = quotes.remove("600000.SH").unwrap();
    println!(
        "{} last={} prev_close={}",
        q.symbol, q.last_price, q.prev_close
    );
    println!("region={} session={:?}", q.region.as_str(), q.session);

    // 2) 多标的（POST，内部按 500 个切片并发拉取）
    let quotes = tf
        .quotes()
        .batch_symbols(["600000.SH", "000001.SZ", "AAPL.US"])
        .send()
        .await?;
    for (symbol, q) in &quotes {
        println!("{symbol}: {} (region={})", q.last_price, q.region.as_str());
    }

    // 3) 按 Universe 拉取（一次性拿到整个沪深 A 股）
    let quotes = tf.quotes().universe("CN_Equity_A").send().await?;
    println!("CN_Equity_A 共 {} 条行情", quotes.len());

    // 4) 多个 Universe（POST）
    let quotes = tf
        .quotes()
        .batch_universes(["CN_Equity_A", "CN_ETF"])
        .send()
        .await?;

    println!("合并 Universe 后共 {} 条行情", quotes.len());

    Ok(())
}
