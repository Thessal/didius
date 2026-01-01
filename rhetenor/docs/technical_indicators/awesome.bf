# AO = SMA(5, median) – SMA(34, median)
# median = (high + low) / 2
awesome_oscillator(high: Signal<Float>, low: Signal<Float>, period1: Int, period2: Int) : Signal<Float> = {
    # Oscillator based on mid price.
    # Typical parameters: period1 = 5, period2 = 34 
    mid : Signal<Float> = divide(dividend=add(x=high, y=low), divisor=2.)
    sma_st : Signal<Float> = ts_mean(signal=median_price, period=period1)
    sma_lt : Signal<Float> = ts_mean(signal=median_price, period=period2)
    result : Signal<Float> = subtract(x=sma_st, y=sma_lt)
}