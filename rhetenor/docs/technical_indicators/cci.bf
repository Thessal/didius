# Commodity Channel Index (CCI) 
cci(
    high   : Signal<Float>,
    low    : Signal<Float>,
    close  : Signal<Float>,
    period : Int
) : Signal<Float> = {
    # Typical price = (high + low + close) / 3
    sum_hl   : Signal<Float> = add(x=high, y=low)
    sum_hlc  : Signal<Float> = add(x=sum_hl, y=close)
    typical  : Signal<Float> = divide(dividend = sum_hlc, divisor = 3.)
    ma_typical : Signal<Float> = ts_mean(signal = typical, period = period)

    # Rolling MAE (rolling average of absolute deviation)
    avg_typical = ts_mae(signal=typical, period=period)

    # CCI = (typical – rolling mean) / ( 0.015 * rolling deviation )
    diff : Signal<Float> = subtract(x = typical, y = ma_typical)
    denom : Signal<Float> = multiply(x=std_typical, y=0.015)
    result : Signal<Float> = divide(dividend = diff, divisor = denom)
}