use std::sync::Arc;

use tokio::sync::watch;

use crate::market::state::MarketState;
use crate::market::orderbook::OrderBook;

pub struct BookProcessor {
    book: OrderBook,
    symbol: String,
    synced: bool,
    publisher: watch::Sender<Option<Arc<MarketState>>>
}