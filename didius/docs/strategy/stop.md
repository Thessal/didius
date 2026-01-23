# Stop Strategy Execution Logic

The `StopStrategy` is a generic execution strategy designed to manage an active order by monitoring specific trigger conditions (Price or Time). Upon triggering, it modifies the original order price with new secondary order price for the remaining unfilled quantity. This is useful for Time-based execution logic (e.g., "Market on Close" or "Limit then Market").

## 1. Overview

- **File**: `src/strategy/stop.rs`
- **Struct**: `StopStrategy`
- **Goal**: Monitor an existing order (`original_order_id`) and switch to a secondary order (`stop_limit_price`) if a condition is met.

## 2. Configuration Parameters

The strategy is initialized with the following parameters:

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `original_order_id` | `String` | The ID of the primary order being managed. |
| `original_symbol` | `String` | Symbol of the primary order (e.g., "005930"). |
| `original_side` | `OrderSide` | Side of the primary order (`BUY` or `SELL`). |
| `original_qty` | `i64` | Total quantity of the primary order. |
| `trigger_price` | `Decimal` | The price threshold that activates the strategy. |
| `trigger_timestamp` | `f64` | Unix timestamp (seconds) for time-based triggering. Set to `0.0` to disable. |
| `stop_limit_price` | `Option<Decimal>` | Price for the secondary order. If `None`, the new order is placed as a Market Order (depending on OMS implementation of `None` price). |

## 3. Trigger Logic

The strategy checks for triggers on every **Order Book Update** (`on_order_book_update`) and **Timer Tick** (`on_timer`).

### A. Time Trigger
- **Condition**: If `trigger_timestamp > 0.0` AND `Current Local Time >= trigger_timestamp`.
- **Action**: Activates the trigger immediately.

### B. Price Trigger
The price trigger depends on `original_side`:

- **BUY Side Trigger (`original_side == OrderSide::BUY`)**
  - **Logic**: Monitors the **Best Bid**.
  - **Condition**: `Best Bid >= trigger_price`.
  - **Use Case**: Stop Buy (Stop Entry)

- **SELL Side Trigger (`original_side == OrderSide::SELL`)**
  - **Logic**: Monitors the **Best Ask**.
  - **Condition**: `Best Ask <= trigger_price`.
  - **Use Case**: Stop Sell (Stop Entry)

## 4. Execution Workflow

1.  **Monitoring**: The strategy passively monitors Market Data and Time.
2.  **Activation**: When a trigger condition is Met:
    - Sets internal state `triggered = true`.
    - Returns `StrategyAction::ModifyPrice(original_order_id, stop_limit_price)`.

## 5. Lifecycle termination

The strategy considers itself finished (`finished = true`) and stops monitoring when:
- The **Order** is `FILLED` (State `OrderState::FILLED`).

## 6. Example Scenarios

### Scenario: Timed Execution (Limit then Market)
- **Goal**: Try to buy at Limit 100. If not filled by 15:00:00, buy immediately at Market/Aggressive.
- **Setup**:
    - Original Order: Buy Limit @ 100.
    - Strategy: `trigger_timestamp` = 15:00:00 (Unix). `stop_limit_price` = Aggressive (e.g., 105 or None).
- **Flow**: at 15:00:00, Strategy modifes Limit @ 100 to Limit @ 105.