rsi(signal: Signal<Float>, lookback: Int) : Signal<Float> = {
    # 1. price change
    d   : Signal<Float> = ts_diff(signal = signal, periods = 1)

    # 2. gains / losses
    g   : Signal<Float> = clip(signal = d, lower = 0., upper = 1e9)
    l   : Signal<Float> = clip(signal = d, lower = -1e9, upper = 0.)

    # 3. moving averages 
    ag  : Signal<Float> = ts_decay_exp(signal = g, period = lookback)
    al  : Signal<Float> = ts_decay_exp(signal = l, period = lookback)

    # 4. relative strength
    rs_ : Signal<Float> = divide(dividend = ag, divisor = al)

    # 5. final RSI
    rs1 : Signal<Float> = divide(dividend =1., divisor= add(x = rs_, y = one))
    result : Signal<Float> = subtract(x = 1., y = rs1)
}

is_peak(signal: Signal<Float>, lookback: Int) : Singal<Float> = {
    result: Signal<Float> = equals(x=ts_max(signal=signal, period=3), y=ts_delay(signal=signal, period=1))
}
is_trough(signal: Signal<Float>, lookback: Int) : Singal<Float> = {
    result: Signal<Float> = equals(x=ts_min(signal=signal, period=3), y=ts_delay(signal=signal, period=1))
}

rsi_divergence(price: Signal<Float>, lookback: Int) : Signal<String> = {
    r : Signal<Float> = rsi(signal = price, lookback = lookback)

    # 2. detect peaks / troughs
    price_peak : Signal<Bool> = is_peak(signal=price, lookback=lookback)
    rsi_peak   : Signal<Bool> = is_peak(signal=r, lookback=lookback)
    price_trough : Signal<Bool> = is_trough(signal=price, lookback=lookback)
    rsi_trough   : Signal<Bool> = is_trough(signal=r, lookback=lookback)

    # 3. forward‑fill the peak/trough flags
    pf : Signal<Bool> = bool_ffill(signal = price_peak)
    rf : Signal<Bool> = bool_ffill(signal = rsi_peak)
    pt : Signal<Bool> = bool_ffill(signal = price_trough)
    rt : Signal<Bool> = bool_ffill(signal = rsi_trough)

    # 4. bearish divergence: higher price peak + lower RSI peak
    bear_cond : Signal<Bool> = and(
        gt(x = pf, y = shift(signal = pf, periods = 1)),
        lt(x = rf, y = shift(signal = rf, periods = 1))
    )
    # 5. bullish divergence: lower price trough + higher RSI trough
    bull_cond : Signal<Bool> = and(
        lt(x = pt, y = shift(signal = pt, periods = 1)),
        gt(x = rt, y = shift(signal = rt, periods = 1))
    )

    # 6. final label
    result : Signal<String> = where(
        condition = bear_cond,
        then = -1.,
        else = where(
            condition = bull_cond,
            then = 1.,
            else = 0.
        )
    )
}