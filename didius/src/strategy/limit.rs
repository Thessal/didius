use crate::oms::order::{Order, OrderState, OrderSide, OrderType, ExecutionStrategy};
use crate::oms::order_book::OrderBook;
use crate::strategy::base::{Strategy, StrategyAction};
use anyhow::Result;
use rust_decimal::Decimal;

pub struct LimitStrategy {
    pub original_order_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: i64,
    pub price: Decimal,
    
    pub finished: bool,
}

impl LimitStrategy {
    pub fn new(
        original_order_id: String,
        symbol: String,
        side: OrderSide,
        quantity: i64,
        price: Decimal,
    ) -> Self {
        LimitStrategy {
            original_order_id,
            symbol,
            side,
            quantity,
            price,
            finished: false,
        }
    }
}

impl Strategy for LimitStrategy {
    fn on_order_book_update(&mut self, _book: &OrderBook) -> Result<StrategyAction> {
        Ok(StrategyAction::None)
    }
    
    fn on_trade_update(&mut self, _price: f64) -> Result<StrategyAction> {
        Ok(StrategyAction::None)
    }
    
    fn on_timer(&mut self) -> Result<StrategyAction> {
        Ok(StrategyAction::None)
    }
    
    fn on_order_status_update(&mut self, order: &Order) -> Result<StrategyAction> {
        if order.order_id.as_deref() == Some(&self.original_order_id) {
             match order.state {
                 OrderState::FILLED | OrderState::CANCELED | OrderState::REJECTED => {
                     self.finished = true;
                 },
                 _ => {}
             }
        }
        Ok(StrategyAction::None)
    }

    fn is_completed(&self) -> bool {
        self.finished
    }

    fn get_origin_order_id(&self) -> Option<String> {
        Some(self.original_order_id.clone())
    }
}
