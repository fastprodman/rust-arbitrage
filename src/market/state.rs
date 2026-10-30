use rust_decimal::Decimal;
use std::time::Instant;

#[derive(Clone)]
pub struct MarketState {
    pub symbol: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub received_at: Instant,
}

#[derive(Clone)]
pub struct PriceLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}