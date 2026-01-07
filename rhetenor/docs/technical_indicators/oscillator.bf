price_oscillator : Signal<Float>(price: Signal<Float>,
                                 short_period: Float,
                                 long_period: Float) = {
    short_ema : Signal<Float> = ema(signal=price, period=short_period)
    long_ema  : Signal<Float> = ema(signal=price, period=long_period)
    result : Signal<Float> = divide(
                                 dividend = subtract(x=short_ema, y=long_ema),
                                 divisor  = long_ema)
}