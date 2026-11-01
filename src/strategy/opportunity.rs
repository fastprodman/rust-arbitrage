use crate::market::exchange::Exchange;
use rust_decimal::Decimal;

pub struct Opportunity {
    pub buy_exchange: Exchange,
    pub sell_exchange: Exchange,
    pub quantity: Decimal,
    pub buy_price: Decimal,
    pub sell_price: Decimal,
    pub expected_profit: Decimal,
}