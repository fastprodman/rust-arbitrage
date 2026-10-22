mod market;
mod gateway;

use crate::gateway::binance::BinanceGateway;
use crate::gateway::coinbase::CoinbaseGateway;

#[tokio::main]
async fn main() -> anyhow::Result<()>{
    let binance_gateway = BinanceGateway::new(
        "wss://stream.binance.com:9443/ws/btcusdt@bookTicker".to_string(),
    );

    let (mut receiver, task) = binance_gateway.run().await?;

    while let Some(quote) = receiver.recv().await {
        println!("Quote is: {quote:?}");
    }

    task.await??;

    Ok(())
}
