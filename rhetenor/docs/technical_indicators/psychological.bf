# psychological_line = ((price - lowest_low) / (highest_high - lowest_low))
psychological_line(price: Signal<Float>, period: Int) : Signal<Float> = {
    highest_high : Signal<Float> = ts_max(signal = price, period = period)
    lowest_low   : Signal<Float> = ts_min(signal = price, period = period)
    price_minus_low : Signal<Float> = subtract(x = price, y = lowest_low)
    range_high_low  : Signal<Float> = subtract(x = highest_high, y = lowest_low)
    # (price - lowest_low) / (highest_high - lowest_low)
    result : Signal<Float> = divide(dividend = price_minus_low, divisor = range_high_low)
}