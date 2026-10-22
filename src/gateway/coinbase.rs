use crate::market::quote::Quote;

use anyhow::{Ok, Result, bail};
use futures_util::{SinkExt, join};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::time::{Instant, SystemTime};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{connect_async, tungstenite::{self, Message}};

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
    tickers: Vec<CoinbaseTicker>
}

#[derive(Deserialize)]
struct CoinbaseTicker {
    product_id: String,

    best_bid: Decimal,
    best_bid_quantity: Decimal,

    best_ask: Decimal,
    best_asl_quantity: Decimal,
}

pub struct CoinbaseGateway {
    coinbase_path: String,
    product_id: String,
}

impl CoinbaseGateway {
    pub fn new(
        coinbase_path: String,
        product_id: String,
    ) -> Self {
        Self{
            coinbase_path,
            product_id,
        }
    }

    pub async fn run(self) -> Result<(mpsc::Receiver<Quote>, JoinHandle<Result<()>>)>{
        let (mut ws, _) = connect_async(&self.coinbase_path).await?;

        let subscription = serde_json::json!({
            "type": "subscribe",
            "product_ids": [self.product_id],
            "channel": "ticker"
        });

        ws.send(Message::Text(subscription.to_string().into()))

        Ok(())
    }
}