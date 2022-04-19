from datetime import date
from typing import Generator

import alpaca_trade_api as tradeapi
from alpaca_trade_api.common import URL
from alpaca_trade_api.rest import TimeFrame, TimeFrameUnit
from more_itertools import ichunked

from src.models.repository.bars import BarDB
from src.models.repository.news import NewsDb


class AlpacaClient:
    lowest_start_at = date(2015, 12, 1)

    def __init__(self, key_id: str, base_url: URL, secret_key: str):
        self.api = tradeapi.REST(key_id=key_id, base_url=base_url, secret_key=secret_key)

    def test_account(self):
        account = self.api.get_account()
        print(account.status)

    def load_all_bars(self, symbol: str, timeframe, start_at, end_at):
        bars = self.api.get_bars(symbol, timeframe, start_at, end_at)
        return bars.df

    def iter_all_bars(self, symbol: str, timeframe, start_at, end_at, raw=False):
        bar_iter = self.api.get_bars_iter(symbol, timeframe, start_at, end_at, raw=raw, adjustment='split')  # prices adjusted for splits (to be continuous)
        return bar_iter

    def download_5_min(self, symbol, end_at: date, start_at: date = None) -> Generator:
        timeframe = TimeFrame(5, TimeFrameUnit.Minute)
        bar_iter = self.iter_all_bars(symbol, timeframe, start_at or self.lowest_start_at, end_at, raw=False)
        for bar_chunk in ichunked(bar_iter, 5_000):
            bars_db = [BarDB.create_from(bar, symbol, '5min') for bar in bar_chunk]
            yield bars_db

    def download_news(self, symbol, end_at: date, start_at: date = None, include_content: bool = False) -> Generator:
        news_iter = self.api.get_news_iter(symbol, start_at or self.lowest_start_at, end_at, limit=None, include_content=include_content)
        for news_chunk in ichunked(news_iter, 5_000):
            news_db = [NewsDb.create_from(news, symbol) for news in news_chunk]
            yield news_db


alpaca = AlpacaClient(
        base_url=URL('https://paper-api.alpaca.markets'),
        key_id='xxx',
        secret_key='xxx',
    )


if __name__ == '__main__':
    # bars = alpaca.load_all_bars(
    #     'AAPL',
    #     TimeFrame(5, TimeFrameUnit.Minute),
    #     date(2017, 7, 4).isoformat(),
    #     date(2017, 7, 3).isoformat(),
    # )
    # print(bars)

    bars = alpaca.iter_all_bars(
        'AAPL',
        TimeFrame(5, TimeFrameUnit.Minute),
        start_at=date(2017, 7, 3).isoformat(),
        end_at=date(2017, 7, 4).isoformat(),
        raw=False
    )
    print(list(bars))

    # for symbol in get_symbols():
    #     for bar in alpaca.iter_all_bars(symbol, TimeFrame.Day, '2010-01-01', '2016-01-01', raw=False):
    #         if bar.t.date() != date(2015,12,1):
    #             print(symbol)
    #             print(bar)
    #         break
