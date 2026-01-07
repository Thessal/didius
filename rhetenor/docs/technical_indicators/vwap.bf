typical_price(close: Signal<Float>, high: Signal<Float>, low: Signal<Float>) 
               : Signal<Float> = {
    sum3   : Signal<Float> = add(x = add(x = close, y = high), y = low) 
    tp     : Signal<Float> = divide(dividend = sum3, divisor = 3.) 
    result : Signal<Float> = tp
}

vwap : Signal<Float> (tp:Signal<Float>, volume:Signal<Float>, lookback: Int) = {
    tpv          : Signal<Float> = multiply(x = tp, y = volume)

    # Running totals
    cum_tpv      : Signal<Float> = ts_decay(signal = tpv, period=lookback)
    cum_volume   : Signal<Float> = ts_decay(signal = volume, period=lookback)

    result       : Signal<Float> = divide(dividend = cum_tpv, divisor = cum_volume)
}