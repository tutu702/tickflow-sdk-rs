use std::env;

use tickflow::client::TickFlow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("TICKFLOW_API_KEY").expect("TICKFLOW_API_KEY must be set");
    let tf = TickFlow::new(api_key)?;

    // 单标的
    let inst = tf.instruments().get("600000.SH").send().await?;
    println!(
        "{} {} on {}",
        inst.symbol,
        inst.name.unwrap_or_default(),
        inst.exchange
    );

    // 批量
    let insts = tf
        .instruments()
        .batch(["600000.SH", "000001.SZ", "AAPL.US"])
        .send()
        .await?;
    for inst in &insts {
        println!("{} {} ({:?})", inst.symbol, inst.code, inst.r#type);
    }

    Ok(())
}
