use std::env;

use tickflow::client::TickFlow;
use tickflow::resources::stream::Channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("TICKFLOW_API_KEY").expect("TICKFLOW_API_KEY must be set");
    let tf = TickFlow::new(api_key)?;

    let stream = tf.stream().clone();

    stream.on_quotes(|quotes| {
        for q in quotes {
            println!(
                "[quote] {}: {} (region={})",
                q.symbol, q.last_price, q.region
            );
        }
    });
    stream.on_depth(|depths| {
        for d in depths {
            if let (Some(bid), Some(ask)) = (d.bid_prices.first(), d.ask_prices.first()) {
                println!("[depth] {} bid1={} ask1={}", d.symbol, bid, ask);
            }
        }
    });
    stream.on_error(|err| {
        eprintln!("[stream] {err}");
    });

    // Queue the subscriptions. They are flushed automatically on
    // connect, and replayed after every reconnect.
    stream.subscribe(
        Channel::Quotes,
        ["600000.SH".to_string(), "000001.SZ".to_string()],
    );
    stream.subscribe(Channel::Depth, ["600000.SH".to_string()]);

    // Drive the connection loop until `close()` is called or a fatal
    // error is reported. Ctrl+C will terminate the runtime.
    let _ = stream.connect().await;

    Ok(())
}
