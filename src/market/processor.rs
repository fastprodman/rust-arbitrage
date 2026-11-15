use std::{sync::Arc, time::Instant};

use crate::market::{
    quote::Quote, 
    state::MarketState,
    book_update::BookEvent,
};
use anyhow::Result;
use tokio::sync::watch;

pub trait BookEventProcessor: Send + 'static {
    fn process_book_event(
        &mut self,
        event: BookEvent,
        received_at: Instant,
    ) -> Result<()>;
}

pub trait MarketProcessor: Send + Sync + 'static {
    fn process_quote(&self, quote: Quote) -> Result<()>;
}

pub struct QuoteProcessor {
    update_chan: watch::Sender<Option<Arc<MarketState>>>
}

impl QuoteProcessor {
    pub fn new() -> (Self, watch::Receiver<Option<Arc<MarketState>>>) {
        let (send, rec) = watch::channel::<Option<Arc<MarketState>>>(None);
        (
            Self {update_chan: send},
            rec,
        )
    }
}

impl MarketProcessor for QuoteProcessor{
    fn process_quote(
        &self,
        quote: Quote,
    ) -> Result<()> {
        self.update_chan.send_replace(Some(Arc::new(quote.into())));

        Ok(())
    }
}