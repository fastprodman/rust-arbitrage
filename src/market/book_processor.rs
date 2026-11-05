use std::sync::Arc;

use tokio::sync::watch;

use crate::market::state::MarketState;
use crate::market::orderbook::OrderBook;

pub struct BookProcessor {
    book: OrderBook,
    symbol: String,
    depth: usize,
    synced: bool,
    publisher: watch::Sender<Option<Arc<MarketState>>>
}

impl BookProcessor {
    pub fn new(
        symbol: String,
        depth: usize,
    ) -> (
        Self,
        watch::Receiver<Option<Arc<MarketState>>>,
    ) {
        let (publisher, receiver) = watch::channel(None);

        let processor = Self{
            book: OrderBook::new(),
            symbol,
            synced: false,
            depth,
            publisher,
        };
        
        (processor, receiver)
    }
}