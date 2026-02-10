
import sys
import os
import time
import json
import shutil

# Add target/release and debug to path to import generated module
sys.path.append("/home/jongkook90/antigravity/didius/target/release")
sys.path.append("/home/jongkook90/antigravity/didius/target/debug")

if "didius" in sys.modules:
    del sys.modules["didius"]
    
try:
    # Try importing Client from didius.core
    from didius.core import Client, Order, OrderSide, OrderType, ExecutionStrategy
except ImportError:
    print("Could not import didius.core. Check if module is built and in path or site-packages.")
    # Attempt import via didius package if installed
    try:
        from didius.core import Client, Order, OrderSide, OrderType, ExecutionStrategy
    except ImportError as e:
        print(f"Import failed: {e}")
        sys.exit(1)

print("Module loaded successfully!")

def test_client():
    print("Testing Client (Rust)...")
    
    # Instantiate Client with "mock" venue
    # New Client API uses venue string.
    client = Client(venue="mock")
    
    print("Client instantiated.")
    
    client.connect()
    print("Client connected.")
    
    # Place Order
    # Order struct requires: symbol, side, type, quantity, (optional: price, strategy, params, stop_price)
    # Check Order signature or use kwargs if supported, but typically new() in rust.
    # From previous errors/code, Order likely takes:
    # (symbol, side, order_type, quantity, price=None, strategy=None, strategy_params=None, stop_price=None)
    # Let's try basic args.
    
    order = Order(
        "AAPL",
        OrderSide.BUY,
        OrderType.LIMIT,
        10
    )
    # Optional fields might need to be set via setters or extra args if pyo3 signature allows.
    # Looking at lib.rs/oms/order.rs via previous knowledge (or guessing defaults).
    # Assuming basic constructor works for now.
    
    success = client.place_order(order)
    print(f"Order Placed: {success}")
    
    # Subscribe?
    client.subscribe(["AAPL"])
    
    # Get OrderBook (Returns JSON string)
    book_json = client.get_order_book("AAPL")
    print(f"OrderBook JSON: {book_json}")
    if book_json:
        book = json.loads(book_json)
        assert book["symbol"] == "AAPL"
    
    # Get Account (Returns JSON string)
    # Account ID? Mock adapter might use "default" or similar.
    acc_json = client.get_account_state("default")
    print(f"Account JSON: {acc_json}")
    
    client.disconnect()
    print("Client Tests OK")

if __name__ == "__main__":
    test_client()
    print("\nALL TESTS PASSED")
