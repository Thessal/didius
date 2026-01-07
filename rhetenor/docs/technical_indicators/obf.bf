obv_calc : Signal<Float> (price: Signal<Float>, vol: Signal<Float>, lookback: Int) = {
    diff_price   : Signal<Float> = ts_diff(x=price, period=1)
    dir          : Signal<Float> = greater(signal=diff_price, threshold=0.)
    signed_flow  : Signal<Float> = multiply(x=vol, y=dir)
    result       : Signal<Float> = ts_decay(signal=signed_flow, period=lookback)
}