mod gateway;
mod market;
mod strategy;

use tokio::time::{interval, Duration};

use crate::gateway::binance::BinanceGateway;
use crate::gateway::coinbase::CoinbaseGateway;
use crate::market::processor::QuoteProcessor;




#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (binance_quote_processor, binance_res)  =  QuoteProcessor::new();
    let binance_gateway =
        BinanceGateway::new(
            binance_quote_processor,
            "wss://stream.binance.com:9443/ws/btcusdt@bookTicker".to_string(),
        );

    let (coinbase_quote_processor, coinbase_res) =  QuoteProcessor::new();
    let coinbase_gateway = CoinbaseGateway::new(
       coinbase_quote_processor,
        "wss://advanced-trade-ws.coinbase.com".to_string(),
        "BTC-USD".to_string(),
    );

    let binance_task = binance_gateway.run().await?;
    let coinbase_task = coinbase_gateway.run().await?;

    _ = tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(5));
        loop {
            ticker.tick().await;

            let binance_quote = binance_res.borrow().as_ref().cloned();
            println!("Binance quote: {binance_quote:?}");

            let coinbase_quote = coinbase_res.borrow().as_ref().cloned();
            println!("Coinbase quote: {coinbase_quote:?}");
        }
    });

    tokio::select! {
        result = binance_task => {
            result??;
            println!("Biance connector exited")
        }
        result = coinbase_task => {
            result??;
            println!("Coinbase connector exited")
        }
    }

    Ok(())
}
