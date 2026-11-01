use std::collections::BTreeMap;
use rust_decimal::Decimal;

use crate::market::state::PriceLevel;

pub struct OrderBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self { 
            bids: BTreeMap::new(), 
            asks: BTreeMap::new(),
        }
    }

    pub fn update_bid(
        &mut self,
        price: Decimal,
        quantity: Decimal,
    ) {
        if quantity.is_zero() {
            self.bids.remove(&price);
        }else{
            self.bids.insert(price, quantity);
        }
    }

    pub fn update_ask(
        &mut self,
        price: Decimal,
        quantity: Decimal,
    ) {
        if quantity.is_zero() {
            self.asks.remove(&price);
        } else {
            self.asks.insert(price, quantity);
        }
    }

    pub fn snapshot(
        &self,
        depth: usize,
    ) -> (Vec<PriceLevel>, Vec<PriceLevel>) {
        let bids = self.bids.iter().rev()
            .take(depth)
            .map(|(&price, &quantity)|PriceLevel{price,quantity})
            .collect();

        let asks = self.asks.iter()
            .take(depth)
            .map(|(&price, &quantity)|PriceLevel{price,quantity})
            .collect();

        (bids, asks)
    }
}