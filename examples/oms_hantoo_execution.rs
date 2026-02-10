use didius::adapter::hantoo::HantooAdapter;
use didius::oms::engine::OMSEngine;
use didius::oms::order::{Order, OrderSide, OrderType, OrderState};
use didius::logger::Logger;
use didius::logger::config::{LoggerConfig, LogDestinationInfo};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use rust_decimal::Decimal;
use std::str::FromStr;

fn main() -> anyhow::Result<()> {
    env_logger::init();
    
    let config_path = "auth/hantoo.yaml";
    println!("Initializing OMS Engine with HantooAdapter (Config: {})...", config_path);

    let adapter = Arc::new(HantooAdapter::new(config_path)?);
    adapter.set_debug_mode(true);

    let logger_config = LoggerConfig {
        destination: LogDestinationInfo::Console,
        flush_interval_seconds: 60,
        batch_size: 1024,
    };
    let logger = Arc::new(Mutex::new(Logger::new(logger_config)));
    
    // Engine expects Arc<dyn Adapter>
    let engine = Arc::new(OMSEngine::new(adapter.clone(), 1.0, logger));
    
    println!("Starting OMS...");
    // Passing None for Python, None for account_id (default from config)
    // Note: start takes py: Python. 
    // Wait, OMSEngine::start signature is: pub fn start(&self, _py: Python, account_id: Option<String>) -> PyResult<()>
    // We cannot easily call it from pure Rust if it requires Python token.
    // However, looking at engine.rs: 
    // pub fn start(&self, _py: Python, account_id: Option<String>) -> PyResult<()> { ... }
    // It ignores _py. But we can't construct Python object easily here.
    
    // Refactor Engine to split start logic?
    // Or just manually call internal steps?
    // engine.start calls:
    // 1. adapter.connect()
    // 2. initialize_account if needed
    // 3. logger.start()
    // 4. spawns keep-alive thread (optional for main)
    
    // Set up monitor
    let (tx, rx) = std::sync::mpsc::channel();
    adapter.set_monitor(tx);

    // Use internal pure Rust methods
    engine.start_internal(None)?;
    
    // listener thread
    engine.start_gateway_listener(rx)?; 

    println!("Waiting 3s for WS connection...");
    thread::sleep(Duration::from_secs(3));

    println!("\n--- 1. Status Check ---");
    let orders = engine.get_orders();
    let acc = engine.get_account();
    println!("Status: Orders: {}, Balance: {}", orders.len(), acc.balance);

    println!("\n--- 2. Place Order ---");
    let symbol = "001360".to_string();
    // let price = Decimal::from_str("1400").unwrap();
    let qty = 1;
    
    let mut order = Order::new(
        symbol.clone(),
        OrderSide::BUY,
        OrderType::LIMIT,
        qty,
        Some("1400".to_string()), 
        None,
        None,
        None
    );
    
    println!("Sending Order: {:?}", order);
    let order_id = engine.send_order_internal(order)?;
    println!("Order Sent. ID: {}", order_id);

    println!("\n--- 3. Monitoring Order ---");
    for i in 0..10 {
        thread::sleep(Duration::from_secs(1));
        let orders = engine.get_orders();
        if let Some(o) = orders.get(&order_id) {
            println!("[{}] Order State: {:?}", i, o.state);
            if o.state == OrderState::NEW || o.state == OrderState::FILLED {
                println!("Order Active/Filled on Exchange!");
                break;
            } else if o.state == OrderState::REJECTED {
                 println!("Order Rejected: {:?}", o.error_message);
                 break;
            }
        } else {
            println!("Order not tracked yet...");
        }
    }

    println!("\n--- 4. Cancel Order ---");
    match engine.cancel_order_internal(order_id.clone()) {
        Ok(_) => println!("Cancel Request Sent."),
        Err(e) => println!("Cancel Failed: {}", e),
    }

    thread::sleep(Duration::from_secs(2));
    
    // Verify cancel
    let orders = engine.get_orders();
    if let Some(o) = orders.get(&order_id) {
        println!("Final State: {:?}", o.state);
    }

    engine.stop_internal()?;
    Ok(())
}
