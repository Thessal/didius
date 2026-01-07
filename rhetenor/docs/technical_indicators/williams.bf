
williams_r(
    signal_high   : Signal<Float>,
    signal_low    : Signal<Float>,
    signal_close  : Signal<Float>,
    lookback      : Int 
) : Signal<Float> = {
    # typical lookback is 14
    hh : Signal<Float> = ts_max(signal=signal_high, period=lookback)
    ll : Signal<Float> = ts_min(signal=signal_low, period=lookback)

    num : Signal<Float> = subtract(x=hh, y=signal_close)
    den : Signal<Float> = subtract(x=hh, y=ll)
    ratio : Signal<Float> = divide(dividend=num, divisor=den)

    result : Signal<Float> = multiply(x=ratio, y=-1.)
}