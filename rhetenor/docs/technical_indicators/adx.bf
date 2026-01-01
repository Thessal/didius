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
    result : Signal<Float> = ts_mean(signal=tr(high=high, low=low, close=close), period=period)
}

# Directional Movement (+DI, -DI)
di_plus(high: Signal<Float>, low: Signal<Float>, close: Signal<Float>, period: Int) : Signal<Float> = {
    up_move : Signal<Float> = ts_diff(signal=high, period=1)
    plus_dm : Signal<Float> = where(
        condition = add(greater(signal=up_move, thres=down_move), greater(signal=up_move, thres=0.0)),
        true_val  = up_move,
        false_val = const(value=0.0)
    )
    smoothed_plus_dm : Signal<Float> = ts_mean(signal=plus_dm, period=period)
    atr : Signal<Float> = atr(high=high, low=low, close=cloe, period=period)
    result : Signal<Float> = divide(dividend=smoothed_plus_dm, divisor=atr)
}
di_minus(high: Signal<Float>, low: Signal<Float>, close: Signal<Float>, period: Int) : Signal<Float> = {
    down_move : Signal<Float> = ts_diff(signal=low, period=1)
    minus_dm  : Signal<Float> = where(
        condition = add(less(signal=up_move, thres=down_move), greater(signal=down_move, thres=0.0)),
        true_val  = down_move,
        false_val = const(value=0.0)
    )
    smoothed_minus_dm : Signal<Float> = ts_mean(signal=minus_dm, period=period)
    atr : Signal<Float> = atr(high=high, low=low, close=cloe, period=period)
    result : Signal<Float> = divide(dividend=smoothed_minus_dm, divisor=atr)
}

# ADX
adx(high: Signal<Float>, low: Signal<Float>, close: Signal<Float>, period: Int) : Signal<Float> = {
    # ADX > 0.2 implies trend.

    # DX = (plus_DI - minus_DI) / (plus_DI + minus_DI)
    di_p     : Signal<Float> = di_plus(high=high, low=low, close=close, period=period) 
    di_m     : Signal<Float> = di_minus(high=high, low=low, close=close, period=period) 
    dx       : Signal<Float> = ratio(x=di_p, y=di_m)

    result   : Signal<Float> = ts_mean(signal=dx_value, period=period)
}