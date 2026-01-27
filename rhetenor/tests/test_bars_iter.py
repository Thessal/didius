
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add src to path
sys.path.insert(0, os.path.abspath("src"))
import rhetenor
print(f"Loaded rhetenor from: {rhetenor.__file__}")

from rhetenor.backtest import Bars

def test_bars_iter():
    # Setup data
    # 10 minutes of data
    periods = 10
    start_time = pd.Timestamp("2023-01-01 10:00:00")
    timestamps = [start_time + timedelta(minutes=i) for i in range(periods)]
    fields = ["open", "high", "low", "close", "volume"]
    symbols = ["SYM1", "SYM2"]
    
    # Create MultiIndex (Time, Field)
    index = pd.MultiIndex.from_product([timestamps, fields], names=["time", "field"])
    columns = symbols
    
    data_values = []
    # Pattern: 
    # Open = 100 + i
    # High = 105 + i
    # Low = 95 + i
    # Close = 102 + i
    # Vol = 1000 + i
    for t_idx in range(periods):
        vals = {
            "open": 100 + t_idx,
            "high": 105 + t_idx,
            "low": 95 + t_idx,
            "close": 102 + t_idx,
            "volume": 1000 + t_idx
        }
        for f in fields:
            row_vals = [vals[f], vals[f] * 2] # SYM2 values are double
            data_values.append(row_vals)
            
    df = pd.DataFrame(data_values, index=index, columns=columns)
    
    # Sort index to Ensure (Time, Field) ordering
    df = df.sort_index()

    # Instantiate Bars with 5 minute interval
    # Note: Bars.__init__ requires interval
    
    bars = Bars(df, interval=timedelta(minutes=5))
    
    iterator = iter(bars)
    
    # Collect results
    results = list(iterator)
    
    print(f"Number of intervals yielded: {len(results)}")
    assert len(results) == 2, f"Expected 2 intervals, got {len(results)}"
    
    # Verify first interval (10:00 - 10:05)
    # timestamps: 0, 1, 2, 3, 4
    # Open: first (idx=0) -> 100
    # High: max (idx=0..4) -> 105+4 = 109
    # Low: min (idx=0..4) -> 95
    # Close: last (idx=4) -> 102+4 = 106
    # Volume: sum (idx=0..4) -> sum(1000..1004) = 5010
    
    i1, row1 = results[0]
    print(f"Interval 1: {i1}")
    
    i1, row1 = results[0]
    print(f"Interval 1: {i1}")
    
    # Check SYM1 (Index 0)
    # row1 is now a Dict with keys 'open', 'high', 'low', 'close', 'volume'
    # providing numpy arrays for each.
    
    # SYM1 is at index 0 because columns=symbols in dataframe
    sym_idx = 0
    
    val_open = row1["open"][sym_idx]
    assert val_open == 100, f"SYM1 Open expected 100, got {val_open}"
    
    val_high = row1["high"][sym_idx]
    assert val_high == 109, f"SYM1 High expected 109, got {val_high}"
    
    val_low = row1["low"][sym_idx]
    assert val_low == 95, f"SYM1 Low expected 95, got {val_low}"
    
    val_close = row1["close"][sym_idx]
    assert val_close == 106, f"SYM1 Close expected 106, got {val_close}"
    
    val_vol = row1["volume"][sym_idx]
    assert val_vol == 5010, f"SYM1 Volume expected 5010, got {val_vol}"

    # Check SYM2 (Index 1) just to be sure
    sym2_idx = 1
    val_open_2 = row1["open"][sym2_idx]
    # SYM2 open at T0 is 100*2 = 200
    assert val_open_2 == 200, f"SYM2 Open expected 200, got {val_open_2}"

    print("Interval 1 properties verified.")

    # Test Unsorted Data Error
    print("Testing unsorted data...")
    df_unsorted = df.copy()
    # Swap two timestamps blocks
    # Swap T0 and T1 block?
    # Actually just swapping rows makes it unsorted by time if I sort by something else or manually shuffle.
    # Let's manually constructing unsorted
    timestamps_unsorted = [start_time + timedelta(minutes=1), start_time] 
    idx_unsorted = pd.MultiIndex.from_product([timestamps_unsorted, fields], names=["time", "field"])
    df_uns = pd.DataFrame(np.random.randn(10, 2), index=idx_unsorted, columns=symbols)
    
    try:
        bars_uns = Bars(df_uns, interval=timedelta(minutes=5))
        list(iter(bars_uns))
        print("Error: Should have raised ValueError for unsorted data")
        sys.exit(1)
    except ValueError as e:
        print(f"Caught expected error: {e}")
        
    print("Test passed!")

if __name__ == "__main__":
    test_bars_iter()
