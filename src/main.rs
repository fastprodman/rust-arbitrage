mod gateway;
mod market;
mod strategy;

use rust_decimal::Decimal;

use crate::gateway::binance::BinanceGateway;
use crate::gateway::coinbase::CoinbaseGateway;
use crate::market::exchange::Exchange;
use crate::market::processor::QuoteProcessor;
use crate::strategy::calculator::ArbitrageService;
use crate::strategy::runner::{MarketDataSource, StrategyRunner};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (binance_quote_processor, binance_res)  =  QuoteProcessor::new();
    let binance_gateway =
        BinanceGateway::new(
            binance_quote_processor,
            "wss://stream.binance.com:9443/ws/btcusd@bookTicker".to_string(),
        );

    let (coinbase_quote_processor, coinbase_res) =  QuoteProcessor::new();
    let coinbase_gateway = CoinbaseGateway::new(
       coinbase_quote_processor,
        "wss://advanced-trade-ws.coinbase.com".to_string(),
        "BTC-USD".to_string(),
    );

    let binance_task = binance_gateway.run().await?;
    let coinbase_task = coinbase_gateway.run().await?;

    let mut runner = StrategyRunner::new(
        ArbitrageService::new(
            Decimal::from_str_exact("0.001")?,
            Decimal::from_str_exact("0.001")?,
            Decimal::from_str_exact("0.01")?,
        ),
        MarketDataSource{
            exchange: Exchange::Coinbase,
            data_receiver: coinbase_res,
        },
        MarketDataSource{
            exchange: Exchange::Binance,
            data_receiver: binance_res,
        },
    );

    let strategy_task = tokio::spawn(async move {
        runner.run().await
    });

    tokio::select! {
        result = binance_task => {
            result??;
            anyhow::bail!("Biance connector exited")
        }
        result = coinbase_task => {
            result??;
            anyhow::bail!("Coinbase connector exited")
        }
        result = strategy_task => {
            result??;
            anyhow::bail!("Strategy task exited")
        }
    }
}
