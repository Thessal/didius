# Aroon Oscillator = ts_argmin - ts_argmax
aroon(signal: Signal<Float>, lookback: Int) : Signal<Float> = {
    # Detects trend change or event.
    # Normalizes data range to -1 ~ 1, and changes rank significantly. 
    return : Signal<Float> = ts_argminmax(signal=signal, period=lookback)
}