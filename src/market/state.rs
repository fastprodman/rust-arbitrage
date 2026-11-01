use rust_decimal::Decimal;
use std::time::{Instant, Duration};

use crate::market::quote::{self, Quote};

#[derive(Clone)]
pub struct MarketState {
    pub symbol: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub received_at: Instant,
}

impl MarketState {
    pub fn is_fresh(&self, max_age: Duration) -> bool {
        self.received_at.elapsed() <= max_age
    }

    pub fn is_valid(&self) -> bool {
        let (Some(bid), Some(ask)) = (self.bids.first(), self.asks.first()) else {
            return false;
        };

        bid.price > Decimal::ZERO
            && ask.price > Decimal::ZERO
            && bid.quantity > Decimal::ZERO
            && ask.quantity > Decimal::ZERO
            && bid.price <= ask.price
    }
}

#[derive(Clone)]
pub struct PriceLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

impl From<Quote> for MarketState {
    fn from(quote: Quote) -> Self {
        Self { 
            symbol: quote.symbol, 
            bids: vec![
                PriceLevel{
                    price: quote.bid_price,
                    quantity: quote.bid_quantity,
                }
            ], 
            asks: vec![
                PriceLevel{
                    price: quote.ask_price,
                    quantity: quote.ask_quantity,
                }
            ], 
            received_at: quote.received_at,
        }
    }
}