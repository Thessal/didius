use crate::oms::order::{Order, OrderSide, OrderType, OrderState, ExecutionStrategy};
use crate::oms::order_book::OrderBook;
use crate::strategy::base::{Strategy, StrategyAction};
use anyhow::Result;
use rust_decimal::prelude::*;
use chrono::Local;

pub struct StopStrategy {
    pub original_order_id: String,
    pub original_symbol: String,
    pub original_side: OrderSide,
    pub original_qty: i64,      // Total Qty
    
    // Trigger logic derived from original_side
    // BUY: Monitor Bid >= Trigger
    // SELL: Monitor Ask <= Trigger
    pub trigger_price: Decimal,
    pub trigger_timestamp: f64,
    
    pub stop_limit_price: Option<Decimal>, // New Order Price (or None for Market)
    
    pub triggered: bool,
    pub finished: bool,
}

impl StopStrategy {
    pub fn new(
        original_order_id: String,
        original_symbol: String,
        original_side: OrderSide,
        original_qty: i64,
        _trigger_side: OrderSide, // Deprecated/Unused param kept for signature compatibility if needed, but we can remove it if callers update
        trigger_price: Decimal, 
        trigger_timestamp: f64, 
        stop_limit_price: Option<Decimal>
    ) -> Self {
        StopStrategy {
            original_order_id,
            original_symbol,
            original_side,
            original_qty,
            trigger_price,
            trigger_timestamp,
            stop_limit_price,
            triggered: false,
            finished: false,
        }
    }

    fn check_trigger(&mut self, book: Option<&OrderBook>) -> bool {
         if self.trigger_timestamp > 0.0 {
             let now = Local::now().timestamp_millis() as f64 / 1000.0;
             if now >= self.trigger_timestamp {
                 return true;
             }
         }
         
         if let Some(b) = book {
              match self.original_side {
                 OrderSide::SELL => {
                     // STOP SELL: Trigger if Best Ask <= Trigger Price
                     if let Some((best_ask, _)) = b.get_best_ask() {
                         if best_ask <= self.trigger_price { return true; }
                     }
                 },
                 OrderSide::BUY => {
                     // STOP BUY: Trigger if Best Bid >= Trigger Price
                     if let Some((best_bid, _)) = b.get_best_bid() {
                         if best_bid >= self.trigger_price { return true; }
                     }
                 }
             }
         }
         
         false
    }
}

impl Strategy for StopStrategy {
    fn on_order_book_update(&mut self, book: &OrderBook) -> Result<StrategyAction> {
        if self.triggered || self.finished {
            return Ok(StrategyAction::None);
        }
        
        if self.check_trigger(Some(book)) {
            self.triggered = true;
            // Return ModifyPrice action
            return Ok(StrategyAction::ModifyPrice(self.original_order_id.clone(), self.stop_limit_price));
        }
        
        Ok(StrategyAction::None)
    }
    
    fn on_trade_update(&mut self, _price: f64) -> Result<StrategyAction> {
        Ok(StrategyAction::None)
    }
    
    fn on_timer(&mut self) -> Result<StrategyAction> {
        if self.triggered || self.finished {
            return Ok(StrategyAction::None);
        }
        
        if self.check_trigger(None) {
            self.triggered = true;
            return Ok(StrategyAction::ModifyPrice(self.original_order_id.clone(), self.stop_limit_price));
        }
        
        Ok(StrategyAction::None)
    }
    
    fn on_order_status_update(&mut self, order: &Order) -> Result<StrategyAction> {
        let order_id = order.order_id.as_deref().unwrap_or("");
        
        if order_id != self.original_order_id {
            return Ok(StrategyAction::None);
        }
        
        // Check for FILLED (Finished naturally)
        if order.state == OrderState::FILLED {
            self.finished = true;
            return Ok(StrategyAction::RemoveOrder(self.original_order_id.clone()));
        }
        
        // If canceled externally or rejected?
        if order.state == OrderState::CANCELED || order.state == OrderState::REJECTED {
             eprintln!("StopStrategy Warning: Order {} Canceled/Rejected. Removing from OMS.", self.original_order_id);
             self.finished = true;
             return Ok(StrategyAction::RemoveOrder(self.original_order_id.clone()));
        }

        Ok(StrategyAction::None)
    }


    fn is_completed(&self) -> bool {
        self.finished
    }

    fn get_origin_order_id(&self) -> Option<String> {
        Some(self.original_order_id.clone())
    }
    
    fn update_order_id(&mut self, new_id: String) {
        self.original_order_id = new_id;
    }
}
