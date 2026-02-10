use anyhow::Result;
use didius::adapter::hantoo::HantooAdapter;
use didius::adapter::Adapter;
use didius::logger::{
    config::{LogDestinationInfo, LoggerConfig},
    Logger,
};
use didius::oms::engine::OMSEngine;
use didius::oms::order::{ExecutionStrategy, Order, OrderSide, OrderState, OrderType};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::io::{self, Write};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use chrono::Local;

fn main() -> Result<()> {
    println!("Initializing HantooAdapter (Stock) for Stop Strategy Test...");
    // Ensure auth/hantoo.yaml exists or adjust path
    let adapter = Arc::new(HantooAdapter::new("auth/hantoo.yaml")?);

    let dest_console = LogDestinationInfo::Console;
    let logger_config = LoggerConfig {
        destination: dest_console,
        flush_interval_seconds: 60,
        batch_size: 1024 * 8,
    };
    let logger = Arc::new(Mutex::new(Logger::new(logger_config)));
    logger.lock().unwrap().start();

    // Margin req can be 1.0 for cash
    let engine = OMSEngine::new(adapter.clone(), logger.clone());

    let (tx, rx) = std::sync::mpsc::channel();
    adapter.set_monitor(tx);
    engine.start_gateway_listener(rx).unwrap();
    
    // User Input: Symbol
    let mut symbol = String::new();
    print!("Enter Stock Symbol (e.g. 005930): ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut symbol)?;
    let symbol = symbol.trim().to_string();
    
    if symbol.is_empty() {
        println!("Symbol cannot be empty.");
        return Ok(());
    }

    // User Input: Tick Size
    let mut tick_str = String::new();
    print!("Enter Tick Size (e.g. 100): ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut tick_str)?;
    let tick_size = Decimal::from_str(tick_str.trim()).unwrap_or(Decimal::new(100, 0));
    println!("Using Symbol: {}, Tick Size: {}", symbol, tick_size);

    adapter.subscribe_market(&[symbol.clone()])?;
    adapter.connect()?;
    adapter.set_debug_mode(false);

    println!("Started. Spawning status printer...");

    let engine_print = engine.clone();
    let symbol_print = symbol.clone();
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(1));
            let orders = engine_print.get_orders();
            let mut summary = String::new();
            summary.push_str(&format!(
                "\n--- OMS Status ---\nActive Orders: {}\n",
                orders.len()
            ));
            for (id, o) in &orders {
                let strategy = o.strategy.clone();
                let strat_name = match strategy {
                   ExecutionStrategy::STOP => "STOP",
                   ExecutionStrategy::NONE => "NONE",
                   _ => "OTHER"
                };
                
                summary.push_str(&format!(
                    "  [{}] {:?} {} @ {} (State: {:?}, Filled: {}) Strategy: {}\n",
                    id, o.side, o.quantity, o.price.map(|p| p.to_string()).unwrap_or("MKT".into()), o.state, o.filled_quantity, strat_name
                ));
            }
            // Simple Book Display
            if let Some(book) = engine_print.get_order_book(&symbol_print) {
                if let Some((bp, bq)) = book.get_best_bid() {
                    summary.push_str(&format!("  Book: Bid {} x {}\n", bp, bq));
                } else {
                     summary.push_str("  Book: Bid None\n");
                }
                if let Some((ap, aq)) = book.get_best_ask() {
                    summary.push_str(&format!("  Book: Ask {} x {}\n", ap, aq));
                } else {
                     summary.push_str("  Book: Ask None\n");
                }
            } else {
                summary.push_str("  Book: None\n");
            }
            println!("{}", summary);
        }
    });

    println!("Interactive Mode:");
    println!("  [b] Stop Buy (Trigger Buy)");
    println!("      Places Passive Buy (-1 ticks). Triggers if Bid >= Ask+5 ticks, or 60s passes. Modifies to Aggressive Buy. (+10 tickes)");
    println!("  [s] Stop Sell (Trigger Sell)");
    println!("      Places Passive Sell (+1 ticks). Triggers if Ask <= Bid-5 ticks, or 60s passes. Modifies to Aggressive Sell. (-10 ticks)");
    println!("  [q] Quit");

    loop {
        print!("> ");
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let cmd = input.trim();

        match cmd {
            "b" | "s" => {
                // Get current price
                let best_ask = engine
                    .get_order_book(&symbol)
                    .and_then(|b| b.get_best_ask().map(|(p, _)| p))
                    .unwrap_or(Decimal::ZERO);
                let best_bid = engine
                    .get_order_book(&symbol)
                    .and_then(|b| b.get_best_bid().map(|(p, _)| p))
                    .unwrap_or(Decimal::ZERO);

                // Default if no book
                if best_ask <= Decimal::ZERO || best_bid <= Decimal::ZERO {
                    println!("No Order Book available. Cannot place relative order.");
                    continue;
                }
                let (ref_bid, ref_ask) = (best_bid.min(best_ask), best_bid.max(best_ask));

                // Chain Timeout: 60s (Long enough to see trigger logic or force manual trigger via mock if desired)
                let timeout_timestamp = Local::now().timestamp_millis() as f64 / 1000.0 + 60.0;
                
                let mut params = HashMap::new();
                params.insert("trigger_timestamp".to_string(), timeout_timestamp.to_string());

                if cmd == "b" {
                    println!("Sending Stop Buy Order...");
                    // 1. Initial Order: Buy Limit @ (Bid - 10 ticks) -> Passive
                    // 2. Trigger: Price (BestBid) >= (Ask + 5 ticks) -> Breakout Validation? 
                    //    Or simpler: Trigger if BestBid >= CurrentBid + 1 tick?
                    //    Let's trigger if Best Bid >= Current Ask (Breakout).
                    
                    let passive_price = ref_bid - tick_size;
                    let trigger_price = ref_ask + (tick_size * Decimal::from(5)); // Breakout level
                    let aggressive_price = ref_ask + (tick_size * Decimal::from(10)); // New Price (Deep Limit)

                    println!("  Initial Buy: {}, Trigger: >= {}, New Price: {}", passive_price, trigger_price, aggressive_price);

                    params.insert("chained_price".to_string(), aggressive_price.to_string());
                    params.insert("trigger_side".to_string(), "BUY".to_string()); // Ignored by logic but used by Engine parser
                    params.insert("trigger_price".to_string(), trigger_price.to_string());

                    let order = Order::new(
                        symbol.clone(),
                        OrderSide::BUY,
                        OrderType::LIMIT,
                        1, 
                        Some(passive_price.to_string()), 
                        Some(ExecutionStrategy::STOP),
                        Some(params),
                        None
                    );
                    
                    match engine.send_order_internal(order) {
                        Ok(id) => println!("Order Sent: {}", id),
                        Err(e) => println!("Error sending order: {}", e),
                    }
                } else if cmd == "s" {
                     println!("Sending Stop Sell Order...");
                    // 1. Initial Order: Sell Limit @ (Ask + 10 ticks) -> Passive
                    // 2. Trigger: Price (BestAsk) <= (Bid - 1 tick) -> Breakdown
                    
                    let passive_price = ref_ask + tick_size;
                    let trigger_price = ref_bid - (tick_size * Decimal::from(5));
                    let aggressive_price = ref_bid - (tick_size * Decimal::from(10));

                    println!("  Initial Sell: {}, Trigger: <= {}, New Price: {}", passive_price, trigger_price, aggressive_price);
                    
                    params.insert("chained_price".to_string(), aggressive_price.to_string());
                    params.insert("trigger_side".to_string(), "SELL".to_string());
                    params.insert("trigger_price".to_string(), trigger_price.to_string());
                    
                    let order = Order::new(
                        symbol.clone(),
                        OrderSide::SELL,
                        OrderType::LIMIT,
                        1, 
                        Some(passive_price.to_string()), 
                        Some(ExecutionStrategy::STOP),
                        Some(params),
                        None
                    );
                     match engine.send_order_internal(order) {
                        Ok(id) => println!("Order Sent: {}", id),
                        Err(e) => println!("Error sending order: {}", e),
                    }
                }
            }
            "q" => break,
            _ => {}
        }
    }

    Ok(())
}
