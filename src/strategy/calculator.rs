use crate::strategy::opportunity::Opportunity;
use crate::market::state::MarketState;

use std::sync::Arc;

pub trait StrategyService {
    fn calculate(
        &self, 
        close_exchage_data: Arc<MarketState>,
        remote_exchage_data: Arc<MarketState>,
    ) -> Option<Opportunity>;
}

pub struct ArbitrageService;

impl ArbitrageService {
    pub fn new() -> Self {
        Self{}
    }
}

impl StrategyService for ArbitrageService {
    fn calculate(
        &self, 
        close_exchage_data: Arc<MarketState>,
        remote_exchage_data: Arc<MarketState>,
    ) -> Option<Opportunity> {
        None
    }
}

