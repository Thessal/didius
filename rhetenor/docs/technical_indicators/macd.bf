# MACD line = fast EMA – slow EMA
macd_line : Signal<Float>(close: Signal<Float>, fast_period: Int, slow_period: Int) = {
    fast  : Signal<Float> = ts_decay_exp(signal = close, fast_period = fast_period)
    slow  : Signal<Float> = ts_decay_exp(signal = close, slow_period = slow_period)
    result : Signal<Float> = subtract(x = fast, y = slow)
}

# Signal line = SMA (or EMA) of the MACD line
macd_signal : Signal<Float>(macd: Signal<Float>, signal_period: Int) = {
    result : Signal<Float> = ts_mean(signal = macd, period = signal_period)
}

# Histogram = MACD line – Signal line
macd_histogram : Signal<Float>(macd: Signal<Float>, signal: Signal<Float>) = {
    result : Signal<Float> = subtract(x = macd, y = signal)
}