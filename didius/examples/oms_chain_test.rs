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
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use chrono::Local;

fn main() -> Result<()> {
    println!("Initializing HantooAdapter (Stock)...");
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
    let engine = OMSEngine::new(adapter.clone(), 1.0, logger.clone());

    let (tx, rx) = mpsc::channel();
    adapter.set_monitor(tx);
    adapter.set_debug_mode(true);
    engine.start_gateway_listener(rx).unwrap();

    adapter.connect()?;
    
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

    println!("Started. Spawning status printer...");

    let engine_print = engine.clone();
    let symbol_print = symbol.clone();
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(1));
            // Dump Status
            let orders = engine_print.get_orders();
            let mut summary = String::new();
            summary.push_str(&format!(
                "\n--- OMS Status ---\nActive Orders: {}\n",
                orders.len()
            ));
            for (id, o) in &orders {
                let strategy = o.strategy.clone();
                let strat_name = match strategy {
                   ExecutionStrategy::CHAIN => "CHAIN",
                   ExecutionStrategy::NONE => "NONE",
                   ExecutionStrategy::FOK => "FOK",
                   ExecutionStrategy::IOC => "IOC",
                   _ => "OTHER"
                };
                
                summary.push_str(&format!(
                    "  [{}] {:?} {} @ {} (State: {:?}, Filled: {}) Strategy: {}\n",
                    id, o.side, o.quantity, o.price.map(|p| p.to_string()).unwrap_or("MKT".into()), o.state, o.filled_quantity, strat_name
                ));
            }
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
    println!("  [b] Buy Trigger (Buy -1tick, Trigger @ Ask, Chain Buy +5tick)");
    println!("  [s] Sell Trigger (Sell +1tick, Trigger @ Bid, Chain Sell -5tick)");
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

                if best_ask == Decimal::ZERO || best_bid == Decimal::ZERO {
                    println!("No market data yet.");
                    continue;
                }

                // Chain Timeout: 30s
                let timeout_timestamp = Local::now().timestamp_millis() as f64 / 1000.0 + 30.0;
                println!("Timeout set to: {}", timeout_timestamp);

                if cmd == "b" {
                    // "buy at current bp-1tick" -> Original Order
                    let buy_price = best_bid - tick_size;
                    // "chained with current ap+5tick ... trigger @ Ask"
                    // If price goes up to Ask, we cancel our low bid and buy high.
                    let trigger_price = best_ask; 
                    let chain_price = best_ask + (tick_size * Decimal::from(5));

                    println!(
                        "Placing Chain Order: Buy @ {}, Trigger @ {}, Chain Buy @ {}",
                        buy_price, trigger_price, chain_price
                    );

                    let mut order = Order::new(
                        symbol.clone(),
                        OrderSide::BUY,
                        OrderType::LIMIT,
                        1,
                        Some(buy_price.to_string()),
                        Some(ExecutionStrategy::CHAIN),
                        None, // client_oid auto gen
                        None,
                    );

                    let mut params = HashMap::new();
                    // Trigger: If Price >= Trigger (Ask)
                    params.insert("trigger_price".to_string(), trigger_price.to_string());
                    params.insert("trigger_side".to_string(), "BUY".to_string());
                    params.insert("trigger_timestamp".to_string(), timeout_timestamp.to_string());

                    params.insert("chained_symbol".to_string(), symbol.clone());
                    params.insert("chained_side".to_string(), "BUY".to_string());
                    params.insert("chained_quantity".to_string(), "1".to_string());
                    params.insert("chained_price".to_string(), chain_price.to_string());

                    order.strategy_params = params;

                    if let Ok(oid) = engine.send_order_internal(order) {
                        println!("Order Sent: {}", oid);
                    } else {
                        println!("Failed to send order");
                    }
                } else {
                    // "s"
                    // "sell at current ap+1tick" -> Original Order
                    let sell_price = best_ask + tick_size;
                    // "chained with current bp-5tick ... trigger @ Bid"
                    // If price drops to Bid, we cancel our high ask and sell low.
                    let trigger_price = best_bid;
                    let chain_price = best_bid - (tick_size * Decimal::from(5));

                    println!(
                        "Placing Chain Order: Sell @ {}, Trigger @ {}, Chain Sell @ {}",
                        sell_price, trigger_price, chain_price
                    );

                    let mut order = Order::new(
                        symbol.clone(),
                        OrderSide::SELL,
                        OrderType::LIMIT,
                        1,
                        Some(sell_price.to_string()),
                        Some(ExecutionStrategy::CHAIN),
                        None,
                        None,
                    );

                    let mut params = HashMap::new();
                    // Trigger: If Price <= Trigger (Bid)
                    params.insert("trigger_price".to_string(), trigger_price.to_string());
                    params.insert("trigger_side".to_string(), "SELL".to_string()); // SELL Side trigger checks <=
                    params.insert("trigger_timestamp".to_string(), timeout_timestamp.to_string());

                    params.insert("chained_symbol".to_string(), symbol.clone());
                    params.insert("chained_side".to_string(), "SELL".to_string());
                    params.insert("chained_quantity".to_string(), "1".to_string());
                    params.insert("chained_price".to_string(), chain_price.to_string());

                    order.strategy_params = params;

                    if let Ok(oid) = engine.send_order_internal(order) {
                        println!("Order Sent: {}", oid);
                    } else {
                        println!("Failed to send order");
                    }
                }
            }
            "q" => break,
            _ => {}
        }
    }

    Ok(())
}
