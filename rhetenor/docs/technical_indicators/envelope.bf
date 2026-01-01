envelope_stoploss: Signal<Float> (signal: Signal<Float>, thres: Float, period: Int) = {
    # Envelope-based data masking, for basic stop-loss implementation.
    # Typical threshold is 0.10, Typical period is 50
    sma : Signal<Float> = ts_mean(signal=abs(signal=signal), period=period)
    boundary : Signal<Float> = multiply(x=sma, y=threshold)
    deviation : Signal<Float> = abs(signal=subtract(signal, sma))
    result : where(less(deviation, boundary), signal, nan)
}