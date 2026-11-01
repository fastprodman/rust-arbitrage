use crate::{
    market::{
        exchange::Exchange, 
        state::MarketState,
    }, 
    strategy::{
        calculator::StrategyService, 
    },
};

use tokio::sync::watch::Receiver;
use tokio::select;
use anyhow::{Context, Result};

use std::time::Duration;

use std::{sync::Arc};

pub struct MarketDataSource {
    pub excahge: Exchange,
    pub data_receiver: Receiver<Option<Arc<MarketState>>>,
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
                result.context("Close exchange closed")
            }
            result = self.remote_exchange.data_receiver.changed() => {
                result.context("Remote exchange closed")
            }
        }
    }

    fn evaluate(&self) {
        let Some(close_data) = self.close_exchange.collect_market_state() else {
            return;
        };

        let Some(remote_data) = self.remote_exchange.collect_market_state() else {
            return;
        };

        let max_age = Duration::from_millis(500);

        if !close_data.is_fresh(max_age) || !remote_data.is_fresh(max_age){
            return;
        }

        if !close_data.is_valid() || !remote_data.is_valid(){
            return;
        }

        let Some(_opportunity) = self.strategy_service.calculate(
            close_data, remote_data,
        ) else {
            return;
        };

        println!("Opportunity detected!");
    }

    pub async fn run(&mut self)-> Result<()> {
        self.evaluate();

        loop {
            self.wait_for_market_update().await?;

            self.evaluate();
        }
    }
}