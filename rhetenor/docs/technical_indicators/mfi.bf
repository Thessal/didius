typical_price(signal_high: Signal<Float>,
              signal_low : Signal<Float>,
              signal_close: Signal<Float>) : Signal<Float> = {
    # typical price = (high + low + close) / 3
    sum_hl      : Signal<Float> = add(x=signal_high, y=signal_low)
    sum_hlc     : Signal<Float> = add(x=sum_hl,      y=signal_close)
    result      : Signal<Float> = divide(dividend=sum_hlc,
                                         divisor=const(value=3.0))
}

typ_prc : Signal<Float> = typical_price(signal_high=high, signal_low=low, signal_close=close)
mfi(typ_prc: Signal<Float>, 
    period:Float) : Signal<Float> = {
    # typical period = 14
    typ_prc_d1 : Signal<Float> = ts_delay(signal=typ_prc, periods=1)
    raw_money_flow : Signal<Float> = multiply(x=typ_prc, y=volume)
    #  Positive / Negative money‑flow masks
    positive_flow : Signal<Float> = multiply(x=raw_money_flow, y=greater(x=typ_prc, y=typ_prc_d1))
    negative_flow : Signal<Float> = multiply(x=raw_money_flow, y=less   (x=typ_prc, y=typ_prc_d1))

    pos_sum : Signal<Float> = ts_sum(signal=positive_flow, period=period)
    neg_sum : Signal<Float> = ts_sum(signal=negative_flow, period=period)

    #  Money‑flow ratio (MFR) = positive_sum / negative_sum
    neg_sum_filled : Signal<Float> = ts_ffill(signal=neg_sum, period=period)
    mfr            : Signal<Float> = divide(dividend=pos_sum, divisor=neg_sum_filled)

    # Money Flow Index = 1 – (1 / (1 + MFR))
    result : Signal<Float> = subtract(x=1., y=divide(dividend=1., divisor=add(x=1., y=mfr)))
}