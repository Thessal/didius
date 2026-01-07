tema : Signal<Float>(prices: Signal<Float>, period: Int) = {
    # First EMA
    ema_1 : Signal<Float> = ema(signal=prices, period=period)

    # Second EMA – applied to the result of the first EMA
    ema_2 : Signal<Float> = ema(signal=ema_1, period=period)

    # Third EMA – applied to the result of the second EMA
    ema_3 : Signal<Float> = ema(signal=ema_2, period=period)

    # TEMA = 3*EMA1 – 3*EMA2 + EMA3
    three_times_ema1 : Signal<Float> = multiply(x=ema_1, y=const(value=3.))
    three_times_ema2 : Signal<Float> = multiply(x=ema_2, y=const(value=3.))
    part1 : Signal<Float> = subtract(x=three_times_ema1, y=three_times_ema2)
    result : Signal<Float> = add(x=part1, y=ema_3)
}