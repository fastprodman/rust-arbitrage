use rust_decimal::Decimal;
use std::time::{Instant, SystemTime};

#[derive(Debug, Clone)]
pub struct Quote {
    pub symbol: String,
    pub bid_price: Decimal,
    pub bid_quantity: Decimal,
    pub ask_price: Decimal,
    pub ask_quantity: Decimal,

    pub received_at: Instant,
    pub timestamp: SystemTime,
}
