use crate::oms::order::Order;
use crate::oms::order_book::OrderBook;
use anyhow::Result;
use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub enum StrategyAction {
    PlaceOrder(Order),
    CancelOrder(String), // order_id
    ModifyPrice(String, Option<Decimal>), // order_id, new_price
    RemoveOrder(String), // order_id
    None,
}

pub trait Strategy {
    // Check if the strategy should trigger based on market data (OrderBook updates, Trade updates, etc.)
    fn on_order_book_update(&mut self, book: &OrderBook) -> Result<StrategyAction>;
    fn on_trade_update(&mut self, price: f64) -> Result<StrategyAction>;
    fn on_order_status_update(&mut self, order: &Order) -> Result<StrategyAction> {
        Ok(StrategyAction::None)
    }
    
    fn on_timer(&mut self) -> Result<StrategyAction> {
        Ok(StrategyAction::None)
    }

    fn is_completed(&self) -> bool {
        false
    }

    fn get_origin_order_id(&self) -> Option<String> {
        None
    }
    
    fn update_order_id(&mut self, _new_id: String) {}
}
