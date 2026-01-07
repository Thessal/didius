# Relative Volatility Index (RVI)
rvi(
    signal_close : Signal<Float>,
    signal_high  : Signal<Float>,
    signal_low   : Signal<Float>,
    period       : Int
) : Signal<Float> = {
    # numerator = Close – Low
    num      : Signal<Float> = subtract(x = signal_close, y = signal_low)

    # denominator = High – Low
    den      : Signal<Float> = subtract(x = signal_high, y = signal_low)

    # smooth both parts with a simple moving average
    num_sma  : Signal<Float> = ts_mean(signal = num, period = period)
    den_sma  : Signal<Float> = ts_mean(signal = den, period = period)

    # final RVI = SMA(num) / SMA(den)
    result   : Signal<Float> = divide(dividend = num_sma, divisor = den_sma)
}