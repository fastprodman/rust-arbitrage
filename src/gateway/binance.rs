use std::time::{Instant, SystemTime};

use rust_decimal::Decimal;
use serde::Deserialize;

use crate::market::quote::{Quote};
use crate::market::processor::{MarketProcessor};

use anyhow::Result;
use futures_util::StreamExt;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use std::sync::Arc;

#[derive(Deserialize)]
struct BinanceBookTicker {
    #[serde(rename = "s")]
    symbol: String,

    #[serde(rename = "b")]
    bid_price: Decimal,

    #[serde(rename = "B")]
    bid_quantity: Decimal,

    #[serde(rename = "a")]
    ask_price: Decimal,

    #[serde(rename = "A")]
    ask_quantity: Decimal,
}

impl BinanceBookTicker {
    pub fn into_quote(self, received_at: Instant, timestamp: SystemTime) -> Quote {
        Quote {
            symbol: self.symbol,
            bid_price: self.bid_price,
            bid_quantity: self.bid_quantity,
            ask_price: self.ask_price,
            ask_quantity: self.ask_quantity,
            received_at,
            timestamp,
        }
    }
}

pub struct BinanceGateway<T: MarketProcessor> {
    binance_path: String,
    quote_processor: T,
}

impl <T: MarketProcessor> BinanceGateway<T> {
    pub fn new(
        quote_processor: T,
        binance_path: String,
    ) -> Self {
        Self { 
            binance_path,
            quote_processor,
        }
    }

    pub async fn run(mut self) -> Result<JoinHandle<Result<()>>> {
        let (mut ws, _) = connect_async(&self.binance_path).await?;

        let task: JoinHandle<Result<()>> = tokio::spawn(async move {
            while let Some(result) = ws.next().await {
                let message: Message = result?;

                if let Message::Text(text) = message {
                    let recieved_at = Instant::now();
                    let timestamp = SystemTime::now();

                    match serde_json::from_str::<BinanceBookTicker>(text.as_str()) {
                        Ok(binance_quote) => {
                            let quote = binance_quote.into_quote(recieved_at, timestamp);

                            self.quote_processor.process_quote(quote);
                        }
                        Err(err) => {
                            eprintln!("Failed to deserialize Binance quote: {err}");
                        }
                    }
                }
            }
            Ok(())
        });

        Ok(task)
    }
}
