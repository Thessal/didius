
import pandas as pd
import numpy as np
import datetime
import sys
import os

sys.path.insert(0, os.path.abspath("src"))
import rhetenor.backtest as bt

def test_position_iter():
    # Setup data
    # 5 minutes of data, with updates every minute
    periods = 5
    start_time = pd.Timestamp("2023-01-01 10:00:30")
    timestamps = [start_time + datetime.timedelta(minutes=i) for i in range(periods)]
    symbols = ["SYM1", "SYM2"]
    
    # Position: increasing allocation
    # T0: [100, 200]
    # T1: [110, 210]
    # T2: [120, 220]
    # T3: [130, 230]
    # T4: [140, 240]
    data = []
    for i in range(periods):
        data.append([100 + i*10, 200 + i*10])
        
    df = pd.DataFrame(data, index=timestamps, columns=symbols, dtype=np.float64)
    # Ensure index is sorted
    df = df.sort_index()

    # Instantiate Position with matching interval
    # Interval = 1 minute: rebalance every minute
    pos = bt.Position(df, interval=datetime.timedelta(minutes=1))
    
    iterator = iter(pos)
    
    # We expect 5 rebalances?
    # Or 4 intervals?
    # T0 -> T1: 1st interval.
    # At T0, we rebalance to T0 target.
    # Yield turnover.
    # Then wait(returns) implicitly (in loop simulation).
    # Then T1 -> T2.
    
    results = []
    current_pos = np.zeros(len(symbols))
    
    # Simulate loop
    # Simulate loop
    for item in iterator:
        print(f"Yield: {item}")
        results.append(item)
        
    print(f"Number of yields: {len(results)}")
    
    # Verify first yield (T0)
    # Yields (ts, values) now
    ts0, val0 = results[0]
    expected_val0 = [100, 200]
    np.testing.assert_array_almost_equal(val0, expected_val0)

    # Verify second yield (T1)
    ts1, val1 = results[1]
    expected_val1 = [110, 210]
    np.testing.assert_array_almost_equal(val1, expected_val1)
    
    print("Test passed!")
    
    print("Test passed!")

if __name__ == "__main__":
    test_position_iter()
