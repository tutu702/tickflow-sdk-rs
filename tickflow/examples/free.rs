use tickflow::client::TickFlow;
use tickflow::resources::klines::KlinesBuilderExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 使用免费服务（无需 API key）
    let tf = TickFlow::free()?;

    // 查询日 K 线数据
    let klines = tf.klines().get("600000.SH").count(100).send().await?;
    if let Some(last) = klines.last_close() {
        println!("最新收盘价: {last}");
    }

    // 查询标的信息
    let instruments = tf
        .instruments()
        .batch(["600000.SH", "000001.SZ"])
        .send()
        .await?;
    println!("{instruments:#?}");

    // 查询所有交易所
    let exchanges = tf.exchanges().list().await?;
    for ex in exchanges {
        println!(
            "{}: {} ({} 个标的)",
            ex.exchange,
            ex.region.as_str(),
            ex.count
        );
    }

    Ok(())
}
