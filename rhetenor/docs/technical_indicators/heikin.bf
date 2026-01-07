heikin_ashi_close(
    open  : Signal<Float>,
    high  : Signal<Float>,
    low   : Signal<Float>,
    close : Signal<Float>
) : Signal<Float> = {
    ha_close_sum   : Signal<Float> = add(x=add(x=open, y=high), y=add(x=low, y=close))
    ha_close       : Signal<Float> = divide(dividend=ha_close_sum, divisor=const(value=4.))
    result : Signal<Float> = ha_close
}

heikin_ashi_open(
    open  : Signal<Float>,
    close : Signal<Float>
) : Signal<Float> = {
    # ---- open --------------------------------------------------
    open_lag  : Signal<Float> = ts_delay(signal=open,  period=1)
    close_lag : Signal<Float> = ts_delay(signal=close, period=1)

    ha_open_raw : Signal<Float> = divide(
                         dividend=add(x=open_lag, y=close_lag),
                         divisor=const(value=2.))

    ha_open_fallback : Signal<Float> = divide(
                         dividend=add(x=open, y=close),
                         divisor=const(value=2.))

    result : Signal<Float> = where(
                 condition=open_lag,
                 val_true=ha_open_raw,
                 val_false=ha_open_fallback)
}

heikin_ashi_high(
    open  : Signal<Float>,
    high  : Signal<Float>,
    low   : Signal<Float>,
    close : Signal<Float>
) : Signal<Float> = {
    ha_open  : Signal<Float> =  heikin_ashi_open(open=open, close=close)
    ha_close: Signal<Float> =  heikin_ashi_open(open=open, high=high, low=low, close=close)
    ha_high_tmp : Signal<Float> = max(x=high, y=ha_open)
    result     : Signal<Float> = max(x=ha_high_tmp, y=ha_close)
}

heikin_ashi_low(
    open  : Signal<Float>,
    high  : Signal<Float>,
    low   : Signal<Float>,
    close : Signal<Float>
) : Signal<Float> = {
    ha_open  : Signal<Float> =  heikin_ashi_open(open=open, close=close)
    ha_close: Signal<Float> =  heikin_ashi_open(open=open, high=high, low=low, close=close)
    ha_low_tmp : Signal<Float> = min(x=low, y=ha_open)
    result     : Signal<Float> = min(x=ha_low_tmp, y=ha_close)
}