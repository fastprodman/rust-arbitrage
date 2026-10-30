use crate::{
    market::{
        exchange::Exchange, 
        state::MarketState,
    }, 
    strategy::{
        calculator::StrategyService, 
        opportunity::Opportunity,
    },
};

use tokio::sync::watch::Receiver;
use tokio::select;
use anyhow::{Context, Result};

use std::{result, sync::Arc};

pub struct MarketDataSource {
    excahge: Exchange,
    data_receiver: Receiver<Option<Arc<MarketState>>>,
} 

impl MarketDataSource {
    fn collect_market_state(&self) -> Option<Arc<MarketState>> {
        self.data_receiver.borrow().as_ref().cloned()
    }
}

pub struct StrategyRunner<S: StrategyService> {
    strategy_service: S,
    close_exchange: MarketDataSource,
    remote_exchange: MarketDataSource,
}

impl<S: StrategyService> StrategyRunner<S> {
    pub fn new(
        strategy_service: S,
        close_exchange: MarketDataSource,
        remote_exchange: MarketDataSource,
    ) -> Self {
        Self{
            strategy_service,
            close_exchange,
            remote_exchange,
        }
    }

    async fn wait_for_market_update(&mut self) -> Result<()> {
        select! {
            result = self.close_exchange.data_receiver.changed() => {
                result.context("Close exchage closed")
            }
            result = self.remote_exchange.data_receiver.changed() => {
                result.context("Remote exchage closed")
            }
        }
    }

    pub async fn run(&mut self)-> Result<()> {
        loop {
            self.wait_for_market_update().await?;

            let Some(close_data) = self.close_exchange.collect_market_state() else {
                continue;
            };

            let Some(remote_data) = self.remote_exchange.collect_market_state() else {
                continue;
            };

            let Some(Opportunity) = self.strategy_service.calculate(close_data, remote_data) else {
                continue;
            };

            todo!()
        }
    }
}