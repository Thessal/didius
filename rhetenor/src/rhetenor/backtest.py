from typing import Any, Dict, Optional
from butterflow import lex, Parser, TypeChecker, Builder, Runtime
import numpy as np
import pandas as pd


def initialize_runtime(s3=Optional[Any], add_logret=True):
    # Initialize runtime
    if s3:
        df = pd.DataFrame({k: pd.DataFrame(v["data"], index=v["fields"]).stack(
        ) for k, v in s3.loaded_data_map.items()}).stack()
        df.sort_index()
        fields = ["open", "high", "low", "close", "volume"]
        dfs = {k: df[k].unstack(level=0).sort_index(
            axis=0).sort_index(axis=1).astype(float) for k in fields}
        assert all([(x.index == dfs["close"].index).all()
                   for x in dfs.values()])
        assert all([(x.columns == dfs["close"].columns).all()
                   for x in dfs.values()])
        runtime_data = {f'data("{k}")': v.values for k, v in dfs.items()}
    else:
        runtime_data = {
            f'data("{x}")': np.load(f"data/{x}.npy")
            for x in ["open", "high", "low", "close", "volume"]}
    if add_logret:
        x_close = pd.DataFrame(runtime_data['data("close")']).ffill().values
        x_close_d1 = np.roll(runtime_data['data("close")'], shift=1, axis=0)
        x_close_d1[0] = x_close[0]
        x_logret = np.log(x_close / x_close_d1)
        runtime_data['data("price")'] = x_close
        runtime_data['data("returns")'] = x_logret  # logret
    return Runtime(data=runtime_data)


def compute(runtime, input_code: str, silent=True):
    tokens = lex(input_code)
    parser = Parser(tokens)
    ast = parser.parse()
    checker = TypeChecker(silent=silent)
    checker.check(ast)
    builder = Builder(silent=silent)
    graph = builder.build(ast)
    result = runtime.run(graph)
    return result


def normalize_position(position_input, x_logret):
    # normalize position
    assert (position_input.shape == x_logret.shape)
    position_raw = position_input - \
        np.nanmean(position_input, axis=1, keepdims=True)
    ls = position_raw / \
        np.nansum(np.where(position_raw >= 0, position_raw, np.nan),
                  axis=1, keepdims=True)
    ss = position_raw / \
        np.abs(np.nansum(np.where(position_raw < 0, position_raw, np.nan),
                         axis=1, keepdims=True))
    position_raw = np.where(position_raw >= 0, ls, ss)
    position = np.nan_to_num(position_raw, 0)
    return position_raw, position


class Backtester:
    # TODO
    # transaction fee
    # bid ask
    # gaps
    def __init__(self, trades, quotes, mode="close"):
        if mode == "close":
            pass
        elif mode == "vwap":
            pass
        elif mode == "market":
            pass
        pass
    # def __init__(self, close_p, open_p, adj_close_p=None, adj_open_p=None, split=None):
    #     self.close_p = close_p
    #     self.open_p = open_p
    #     if adj_close_p == None:
    #         self.adj_close_p = close_p
    #     if adj_open_p == None:
    #         self.adj_open_p = open_p
    #     if split == None:
    #         self.split = np.ones_like(self.close_p, dtype=float)
    #     # if dividend_per_share == None:
    #     #     dividend_per_share = np.zeros_like(self.close_p, dtype=float)

    #     self.ret_overnight = (self.adj_open_p[2:] / self.adj_close_p[1:-1])
    #     self.drift_overnight = (
    #         self.open_p[2:] / self.close_p[1:-1]) * self.split[2:]
    #     self.ret_intraday = (self.adj_close_p[2:] / self.adj_open_p[2:])
    #     self.drift_intraday = (self.adj_close_p[2:] / self.adj_open_p[2:])

    # def run(self, position):
    #     assert position.shape == self.close_p.shape

    #     # Position calculated
    #     calc_d2 = position[0:-2]
    #     calc_d1 = position[1:-1]
    #     pos_after_yesterday_close = calc_d2

    #     # Opening trade
    #     pos_before_open = pos_after_yesterday_close * self.ret_overnight
    #     pos_after_open = calc_d1
    #     tvr_open = pos_after_open - pos_before_open
    #     ret_overnight = np.nansum(
    #         pos_before_open - pos_after_yesterday_close, axis=1)

    #     # Closing trade
    #     pos_before_close = pos_after_open * self.ret_intraday
    #     pos_after_close = calc_d1
    #     tvr_close = pos_after_close - pos_before_close
    #     ret_intraday = np.nansum(pos_before_close - pos_after_open, axis=1)

    #     turnover = np.nanmean(np.abs(tvr_open), axis=1) + \
    #         np.nanmean(np.abs(tvr_close), axis=1)
    #     returns = ret_overnight + ret_intraday

    #     return returns, turnover
