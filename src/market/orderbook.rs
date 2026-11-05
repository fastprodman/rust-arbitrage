use std::collections::BTreeMap;
use rust_decimal::Decimal;

use crate::market::state::PriceLevel;
use crate::market::book_update::BookUpdate;

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

    pub fn apply_update(&mut self, update: BookUpdate){
        for bid_update in update.bids {
            self.update_bid(bid_update.price, bid_update.quantity);
        }

        for ask_update in update.asks {
            self.update_ask(ask_update.price, ask_update.quantity);
        }
    }

    pub fn apply_snapshot(&mut self, update: BookUpdate) {
        self.asks.clear();
        self.bids.clear();

        self.apply_update(update);
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