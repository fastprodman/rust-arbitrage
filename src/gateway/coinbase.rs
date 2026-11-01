use crate::market::{
    processor::MarketProcessor,
    quote::Quote
};

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::time::{Instant, SystemTime};
use std::sync::Arc;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, Message},
};

#[derive(Deserialize)]
struct CoinbaseMessage {
    #[serde(default)]
    channel: String,

    #[serde(default)]
    events: Vec<CoinbaseEvent>,

    #[serde(rename = "type")]
    message_type: Option<String>,

    message: Option<String>,
}

#[derive(Deserialize)]
struct CoinbaseEvent {
    #[serde(default)]
    tickers: Vec<CoinbaseTicker>,
}

#[derive(Deserialize)]
struct CoinbaseTicker {
    product_id: String,

    best_bid: Decimal,
    best_bid_quantity: Decimal,

    best_ask: Decimal,
    best_ask_quantity: Decimal,
}

impl CoinbaseTicker {
    fn into_quote(self, received_at: Instant, timestamp: SystemTime) -> Quote {
        Quote {
            symbol: self.product_id,
            bid_price: self.best_bid,
            bid_quantity: self.best_bid_quantity,
            ask_price: self.best_ask,
            ask_quantity: self.best_ask_quantity,
            received_at,
            timestamp,
        }
    }
}

pub struct CoinbaseGateway<T: MarketProcessor> {
    quote_processor: T,
    coinbase_path: String,
    product_id: String,
}

impl<T: MarketProcessor> CoinbaseGateway<T> {
    pub fn new(
        quote_processor: T,
        coinbase_path: String, product_id: String,
    ) -> Self {
        Self {
            quote_processor,
            coinbase_path,
            product_id,
        }
    }

    pub async fn run(self) -> Result<JoinHandle<Result<()>>> {
        let (mut ws, _) = connect_async(&self.coinbase_path).await?;

        let subscription = serde_json::json!({
            "type": "subscribe",
            "product_ids": [self.product_id],
            "channel": "level2"
        });

        ws.send(Message::Text(subscription.to_string().into()))
            .await?;

        // Subscribe to heartbeats
        let heartbeat_subscription = serde_json::json!({
            "type": "subscribe",
            "channel": "heartbeats"
        });

        ws.send(Message::Text(heartbeat_subscription.to_string().into()))
            .await?;

        let task = tokio::spawn(async move {
            while let Some(result) = ws.next().await {
                let message = result?;

                match message {
                    Message::Text(payload) => {
                        let received_at = Instant::now();
                        let timestamp = SystemTime::now();

                        let parsed: CoinbaseMessage = match serde_json::from_str(payload.as_str()) {
                            Ok(coinbase_message) => coinbase_message,
                            Err(err) => {
                                eprintln!("Coinbase error: {err}");
                                continue;
                            }
                        };

                        if parsed.message_type == Some("error".to_string()) {
                            bail!("Coinbase error: {:?}", parsed.message);
                        }

                        if parsed.channel != "ticker".to_string() {
                            continue;
                        }

                        for event in parsed.events {
                            for ticker in event.tickers {
                                let quote = ticker.into_quote(received_at, timestamp);

                                self.quote_processor.process_quote(quote)
                                    .context("failed to process coinbase quote")?;
                            }
                        }
                    }
                    Message::Close(frame) => {
                        bail!("Coinbase closed connection: {frame:?}")
                    }
                    _ => {}
                }
            }

            bail!("coinbase stream ended")
        });

        Ok(task)
    }
}
