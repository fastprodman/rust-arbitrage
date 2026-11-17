
use crate::market::{
    book_update::{BookEvent, BookLevelUpdate, BookUpdate}, 
    processor::BookEventProcessor,
};

use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::time::Instant;
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
};


#[derive(Deserialize)]
#[serde(rename_all="lowercase")]
enum CoinbaseEventType {
    Snapshot,
    Update,
}

#[derive(Deserialize)]
struct CoinbaseL2Update {
    side: CoinbaseSide,
    price_level: Decimal,
    new_quantity: Decimal,
}

#[derive(Deserialize)]
#[serde(rename_all="lowercase")]
enum CoinbaseSide {
    Bid,
    Offer,
}

#[derive(Deserialize)]
struct CoinbaseL2Message {
    channel: String,
    sequence_num: u64,
    events: Vec<CoinbaseL2Event>
}

#[derive(Deserialize)]
struct CoinbaseL2Event {
    #[serde(rename = "type")]
    event_type: CoinbaseEventType,
    product_id: String,
    updates: Vec<CoinbaseL2Update>,
}

pub struct CoinbaseGateway<T: BookEventProcessor> {
    book_processor: T,
    coinbase_path: String,
    product_id: String,
    last_sequence_num: Option<u64>,
}

#[derive(PartialEq)]
enum MessageProcessingResult {
    Proceed,
    Reconcile,
}

impl<T: BookEventProcessor> CoinbaseGateway<T> {
    pub fn new(
        book_processor: T,
        coinbase_path: String, 
        product_id: String,
    ) -> Self {
        Self {
            book_processor,
            coinbase_path,
            product_id,
            last_sequence_num: None,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let (mut ws, _) = connect_async(&self.coinbase_path).await?;

        let subscription = serde_json::json!({
            "type": "subscribe",
            "product_ids": [&self.product_id],
            "channel": "level2",
        });

        ws.send(Message::Text(subscription.to_string().into()))
            .await?;

        let heartbeat = serde_json::json!({
            "type": "subscribe",
            "channel": "heartbeats",
        });

        ws.send(Message::Text(heartbeat.to_string().into()))
            .await?;

        while let Some(result) = ws.next().await {
            match result? {
                Message::Text(payload) => {
                    let result = self.handle_payload(
                        payload.as_str(),
                        Instant::now(),
                    )?;

                    if result == MessageProcessingResult::Proceed {
                        continue;
                    }

                    // TODO: Reconcle
                }

                Message::Close(frame) => {
                    bail!("Coinbase closed connection: {frame:?}");
                }

                _ => {}
            }
        }

        bail!("Coinbase stream ended")
    }

    fn handle_payload(
        &mut self,
        payload: &str,
        received_at: Instant,
    ) -> Result<MessageProcessingResult> {
        let raw: serde_json::Value =
            serde_json::from_str(payload)
                .context("Invalid Coinbase JSON")?;

        if raw.get("type").and_then(|v| v.as_str()) == Some("error") {
            bail!("Coinbase error: {raw}");
        }

        let Some(sequence_num) = raw.get("sequence_num").and_then(|v| v.as_u64()) else {
            bail!("Sequesnce number missing");
        };

        if let Some(last_sequence_num) = self.last_sequence_num {
            if sequence_num > (last_sequence_num + 1) {
                self.book_processor.invalidate_book();

                return Ok(MessageProcessingResult::Reconcile);
            }
        }

        if raw.get("channel").and_then(|v| v.as_str()) != Some("l2_data") {
            return Ok(MessageProcessingResult::Proceed);
        }

        let parsed_message = serde_json::from_value::<CoinbaseL2Message>(raw)
                .context("Invalid Coinbase L2 message")?;

        for event in parsed_message.events {
            if event.product_id != self.product_id {
                continue;
            }

            self.book_processor.process_book_event(
                event.into(),
                received_at,
            )?;
        }

        Ok(MessageProcessingResult::Proceed)
    }
}


impl From<CoinbaseL2Event> for BookEvent {
    fn from(event: CoinbaseL2Event) -> Self {
        let mut bids = Vec::new();
        let mut asks = Vec::new();

        for update in event.updates {
            let level = BookLevelUpdate {
                price: update.price_level,
                quantity: update.new_quantity,
            };

            match update.side {
                CoinbaseSide::Bid => {bids.push(level)}
                CoinbaseSide::Offer => {asks.push(level)}
            };
        }

        let book_update = BookUpdate{bids, asks};

        match event.event_type {
            CoinbaseEventType::Snapshot => {
                BookEvent::Snapshot(book_update)
            }
            CoinbaseEventType::Update => {
                BookEvent::Update(book_update)
            }
        }
    }
}
