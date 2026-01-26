import numpy as np


def calculate_stat(position_raw, position, x_logret):
    # Calculate stat
    returns = np.nansum(position[:-1] * x_logret[1:], axis=1)
    cum_pnl = np.cumsum(returns)
    stat = {
        "min_coverage": np.nanmin(np.nanmean(np.isfinite(position_raw), axis=1)),
        "returns": np.nanmean(returns)*252,
        "sharpe": np.nanmean(returns)/np.nanstd(returns)*np.sqrt(252),
        "max_turnover": np.nanmax(np.nansum(np.abs(np.diff(position, axis=0)), axis=1)),
        "mdd": np.nanmin(np.maximum.accumulate(cum_pnl) - cum_pnl),
        "max_position": np.nanmax(np.abs(position))
    }
    return stat