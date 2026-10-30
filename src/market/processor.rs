use crate::market::quote::Quote;
use anyhow::Result;
use tokio::sync::watch;

pub trait MarketProcessor: Send + Sync + 'static {
    fn process_quote(&self, quote: Quote) -> Result<()>;
}

pub struct QuoteProcessor {
    update_chan: watch::Sender<Option<Quote>>
}

impl QuoteProcessor {
    pub fn new() -> (Self, watch::Receiver<Option<Quote>>) {
        let (send, rec) = watch::channel::<Option<Quote>>(None);
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
        self.update_chan.send_replace(Some(quote));

        Ok(())
    }
}