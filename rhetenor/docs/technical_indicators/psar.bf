# The Parabolic SAR is a trend-following indicator that places dots above or below the price chart, depending on the trend direction. When the market is trending up, the dots appear below the price; when the market is trending down, the dots appear above the price. A dot switching sides signals a potential trend reversal.
psar(high: Signal<Float>, low: Signal<Float>, close: Signal<Float>, lookback: Int, dynamic_lookback: Singal<Float>) : Signal<Float> = {
    # Trend detection – bullish if close > previous close 
    prev_close : Float = ts_delay(signal=close, period=1)
    bullish   : Bool  = greater(signal=close, thres=prev_close)

    # Extreme‑Point (EP) for the current bar
    lowest_low   : Float = ts_min(signal=low , period=lookback)
    highest_high  : Float = ts_max(signal=high, period=lookback)
    ep       : Float = where(condition=bullish, val_true=lowest_low, val_false=highest_high)

    # Approximately Calculate PSAR
    result: Signal<Float> = ts_decay_exp(signal=signal, period: dynamic_lookback)
}