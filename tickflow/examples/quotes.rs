use std::env;
use tickflow::client::TickFlow;
use tickflow::model::QuoteResponse;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("TICKFLOW_API_KEY").expect("TICKFLOW_API_KEY must be set");

    let tf = TickFlow::new(&api_key)?;

    // 1) 单标的（GET）
    let resp = tf.quotes().symbol("600000.SH").send().await?;
    let mut quotes = match resp {
        QuoteResponse::Raw(m) => m,
        QuoteResponse::DataFrame(_) => unreachable!("as_dataframe() was not called"),
    };
    let q = quotes.remove("600000.SH").unwrap();
    println!(
        "{} last={} prev_close={}",
        q.symbol, q.last_price, q.prev_close
    );
    println!("region={} session={:?}", q.region.as_str(), q.session);

    // 2) 多标的（POST，内部按 500 个切片并发拉取）
    let resp = tf
        .quotes()
        .batch_symbols(["600000.SH", "000001.SZ", "AAPL.US"])
        .send()
        .await?;
    let quotes = match resp {
        QuoteResponse::Raw(m) => m,
        QuoteResponse::DataFrame(_) => unreachable!("as_dataframe() was not called"),
    };
    for (symbol, q) in &quotes {
        println!("{symbol}: {} (region={})", q.last_price, q.region.as_str());
    }

    // 3) 按 Universe 拉取（一次性拿到整个沪深 A 股）
    let resp = tf.quotes().universe("CN_Equity_A").send().await?;
    let quotes = match resp {
        QuoteResponse::Raw(m) => m,
        QuoteResponse::DataFrame(_) => unreachable!("as_dataframe() was not called"),
    };
    println!("CN_Equity_A 共 {} 条行情", quotes.len());

    // 4) 多个 Universe（POST）
    let resp = tf
        .quotes()
        .batch_universes(["CN_Equity_A", "CN_ETF"])
        .send()
        .await?;
    let quotes = match resp {
        QuoteResponse::Raw(m) => m,
        QuoteResponse::DataFrame(_) => unreachable!("as_dataframe() was not called"),
    };

    println!("合并 Universe 后共 {} 条行情", quotes.len());

    let resp = tf
        .quotes()
        .batch_universes(["CN_Equity_A", "CN_ETF"])
        .as_dataframe()
        .send()
        .await?;
    let df = match resp {
        QuoteResponse::DataFrame(df) => df,
        QuoteResponse::Raw(_) => unreachable!("as_dataframe() was called"),
    };
    println!("{df}");

    Ok(())
}
