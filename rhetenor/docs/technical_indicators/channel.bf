# Upper Channel Line (UCL) – highest high over the look‑back period
upper_channel(high: Signal<Float>, period: Int) : Signal<Float> = {
    ucl : Signal<Float> = ts_max(signal = high, period = period)
    result : Signal<Float> = ucl
}

# Lower Channel Line (LCL) – lowest low over the look‑back period
lower_channel(low: Signal<Float>, period: Int) : Signal<Float> = {
    lcl : Signal<Float> = ts_min(signal = low, period = period)
    result : Signal<Float> = lcl
}