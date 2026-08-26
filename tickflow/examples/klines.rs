use std::env;

use tickflow::client::TickFlow;
use tickflow::model::{AdjustType, Period};
use tickflow::resources::klines::KlinesBuilderExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("TICKFLOW_API_KEY").expect("TICKFLOW_API_KEY must be set");
    let tf = TickFlow::new(api_key)?;

    // 1) 单标的日 K（默认 1d，count=100，前复权）
    let klines = tf.klines().get("600000.SH").count(100).send().await?;
    println!(
        "K 线 {} 条，最后收盘 = {:?}",
        klines.len(),
        klines.last_close()
    );

    // 2) 自定义周期、复权方式、时间区间
    let klines = tf
        .klines()
        .get("600000.SH")
        .period(Period::D1)
        .count(250)
        .adjust(AdjustType::Forward)
        .send()
        .await?;
    println!("区间 K 线 {} 条", klines.len());

    // 3) 批量拉取
    let klines = tf
        .klines()
        .batch(["600000.SH", "000001.SZ"])
        .count(30)
        .period(Period::D1)
        .send()
        .await?;
    for (symbol, kl) in &klines {
        println!(
            "{symbol}: {} 条, last_close={:?}",
            kl.len(),
            kl.last_close()
        );
    }

    Ok(())
}
