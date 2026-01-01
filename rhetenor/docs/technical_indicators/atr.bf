# True Range (TR)
tr(high: Signal<Float>, low: Signal<Float>, close: Signal<Float>) : Signal<Float> = {
    # Measures directional movements, considering price gaps and daily volatility. 
    close_d1 = ts_delay(signal=close, period=1)
    high_low   : Signal<Float> = subtract(x=high, y=low)
    high_close : Signal<Float> = abs(subtract(x=high, y=close_d1))
    low_close  : Signal<Float> = abs(subtract(x=low, y=close_d1))
    result     : Signal<Float> = max(x=high_low, y=max(x=high_close, y=low_close))
}

# Averaged True Range (ATR)
atr(high: Signal<Float>, low: Signal<Float>, close: Signal<Float>, period: Int) : Signal<Float> = {
    # ATR is commonly used as an exit method
    # Usage Example (Chandelier exit) : Stop-loss at high-3*ATR, or Profit-taking at low+3*ATR
    result : Signal<Float> = ts_mean(signal=tr(high=high, low=low, close=close), period=period)
}