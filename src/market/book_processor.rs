use std::sync::Arc;

use anyhow::Result;
use tokio::sync::watch;

use std::time::Instant;

use crate::market::book_update::BookEvent;
use crate::market::state::MarketState;
use crate::market::orderbook::OrderBook;
use crate::market::processor::BookEventProcessor;

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

    pub fn process(
        &mut self,
        event: BookEvent,
        received_at: Instant,
    ) -> Result<()> {
        match event {
            BookEvent::Snapshot(snapshot) => {
                self.book.apply_snapshot(snapshot);
                self.synced = true;
            }
            BookEvent::Update(update) => {
                if !self.synced {
                    return Ok(());
                }

                self.book.apply_update(update);
            }
        }

        let (bids, asks) = self.book.snapshot(self.depth);

        let state = MarketState {
            symbol: self.symbol.clone(),
            bids,
            asks,
            received_at,
        };

        if !state.is_valid() {
            self.invalidate();
            return Ok(());
        }

        self.publisher.send_replace(Some(Arc::new(state)));

        Ok(())
    }

    pub fn invalidate(&mut self) {
        self.synced = false;
        self.book = OrderBook::new();
        self.publisher.send_replace(None);
    }
}

impl BookEventProcessor for BookProcessor {
    fn process_book_event(
        &mut self,
        event: BookEvent,
        received_at: Instant,
    ) -> Result<()> {
        self.process(event, received_at)
    }
    
    fn invalidate_book(&mut self) {
        self.invalidate();
    }
}