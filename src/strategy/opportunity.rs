use crate::market::exchange::Exchange;
use rust_decimal::Decimal;

pub struct Opportunity {
    buy_exchange: Exchange,
    sell_exchange: Exchange,
    quantity: Decimal,
}