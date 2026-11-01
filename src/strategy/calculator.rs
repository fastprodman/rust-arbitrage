use rust_decimal::Decimal;

use crate::strategy::opportunity::Opportunity;
use crate::market::state::{MarketState, PriceLevel};
use crate::market::exchange::Exchange;

pub trait StrategyService {
    fn calculate(
        &self, 
        near: (Exchange, &MarketState),
        remote: (Exchange, &MarketState),
    ) -> Option<Opportunity>;
}

pub struct ArbitrageService {
    near_taker_fee: Decimal,
    remote_taker_fee: Decimal,
    min_profit: Decimal,
}

impl ArbitrageService {
    pub fn new(
        near_taker_fee: Decimal,
        remote_taker_fee: Decimal,
        min_profit: Decimal,
    ) -> Self {
        Self{
            near_taker_fee,
            remote_taker_fee,
            min_profit,
        }
    }

    fn evaluate_direction(
        &self,
        buy_exchange: Exchange,
        sell_exchange: Exchange,
        ask: &PriceLevel,
        bid: &PriceLevel,
        buy_fee: Decimal,
        sell_fee: Decimal,
    ) -> Option<Opportunity> {
        let quantity = ask.quantity.min(bid.quantity);

        if quantity <= Decimal::ZERO {
            return None;
        }

        let buy_cost = ask.price * quantity;
        let sell_revenue = bid.price * quantity;

        let fees = buy_cost * buy_fee + sell_revenue * sell_fee;

        let expected_profit = sell_revenue - buy_cost - fees;

        if expected_profit <= self.min_profit {
            return None;
        }

        Some(Opportunity {
            buy_exchange,
            sell_exchange,
            quantity,
            buy_price: ask.price,
            sell_price: bid.price,
            expected_profit,
        })
    }
}

impl StrategyService for ArbitrageService {
    fn calculate(
        &self, 
        near: (Exchange, &MarketState),
        remote: (Exchange, &MarketState),
    ) -> Option<Opportunity> {
        let (near_exchange, near_state) = near;
        let (remote_exchange, remote_state) = remote;

        let near_ask = near_state.asks.first()?;
        let near_bid = near_state.bids.first()?;

        let remote_ask = remote_state.asks.first()?;
        let remote_bid = remote_state.bids.first()?;

        let direction_a = self.evaluate_direction(
            near_exchange,remote_exchange,
            near_ask, remote_bid,
            self.near_taker_fee,self.remote_taker_fee,
        );

        let direction_b = self.evaluate_direction(
            remote_exchange,near_exchange,
            remote_ask, near_bid,
            self.remote_taker_fee,self.near_taker_fee,
        );

        match (direction_a, direction_b) {
            (Some(a), Some(b)) => {
                if a.expected_profit >= b.expected_profit {
                    Some(a)
                } else {
                    Some(b)
                }
            },
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }
}

