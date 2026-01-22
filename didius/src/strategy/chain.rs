use crate::oms::order::{Order, OrderSide, OrderType, OrderState};
use crate::oms::order_book::OrderBook;
use crate::strategy::base::Strategy;
use anyhow::Result;
use rust_decimal::prelude::*;
use tokio::time;

pub struct ChainStrategy {
    pub trigger_price_side: OrderSide,
    pub trigger_price: Decimal,
    pub trigger_timestamp: f64,
    pub chained_order: Order,
}

impl ChainStrategy {
    // Original order is canceled and chained order is submitted when the trigger is activited.
    // Trigger is activated if : 
    // * current timestamp >= trigger timestamp 
    // * trigger_price_side is BUY and current bid price > trigger_price 
    // * trigger_price_side is SELL and current ask price < trigger_price
    pub fn new(trigger_price_side: OrderSide, trigger_price: Decimal, trigger_timestamp: f64, chained_order: Order) -> Self {
        ChainStrategy {
            trigger_price_side,
            trigger_price,
            trigger_timestamp,
            chained_order: chained_order,
        }
    }
}

impl Strategy for ChainStrategy {
    fn on_order_book_update(&mut self, book: &OrderBook) -> Result<Option<Order>> {
        // Assumes that the origianl order is submitted 
        let time_trig: bool = chrono::Local::now().timestamp_millis() as f64 / 1000.0 >= self.trigger_timestamp; 
        let price_trig = match self.trigger_price_side {
            OrderSide::SELL => book.get_best_ask().map_or(false, |(p,_q)| p <= self.trigger_price),
            OrderSide::BUY => book.get_best_bid().map_or(false, |(p,_q)| p >= self.trigger_price),
        };

        if time_trig || price_trig {
            let mut o = self.chained_order.clone();
            o.state = OrderState::CREATED; // Reset state so engine processes it as new
            return Ok(Some(o)); // Return chained order to the OMS. OMS should cancel the original order and place chained order.
        }
        Ok(None)
    }
    fn on_trade_update(&mut self, _price: f64) -> Result<Option<Order>> {
        // TODO: enable triggering by last trade price
        Ok(None)
    }
}
