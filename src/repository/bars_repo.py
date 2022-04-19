from collections import defaultdict
from typing import List

import pandas as pd

from src.models.repository.bars import BarDB
from src.repository.base import DatabaseClient


class BarsRepo(DatabaseClient):
    timeframe = '5min'

    def add_bar(self, bar: BarDB):
        self.execute_query(
            f'''
            INSERT INTO bars{self.sample} (symbol, timeframe, datetime_at, open, high, low, close, volume)
            VALUES (?,?,?,?,?,?,?,?)
            ''',
            (bar.symbol, bar.timeframe, bar.datetime_at, bar.open, bar.high, bar.low, bar.close, bar.volume),
        )

    def add_bars(self, bars: List[BarDB]):
        self.execute_many_query(
            f'''
            INSERT INTO bars{self.sample} (symbol, timeframe, datetime_at, open, high, low, close, volume)
            VALUES (?,?,?,?,?,?,?,?)
            ''',
            [(bar.symbol, bar.timeframe, bar.datetime_at, bar.open, bar.high, bar.low, bar.close, bar.volume) for bar in
             bars],
        )

    def load_bars_to_df(self, symbol):
        with self.connection as con:
            df = pd.read_sql_query(
                f'''
                SELECT datetime_at, open, high, low, close
                FROM bars{self.sample}
                WHERE symbol = (?)
                ''',
                con,
                params=(symbol,)
            )
        return df

    def load_bars_with_gaps_to_df(self, symbol, start_at, end_at):  # inclusive - non-inclusive
        with self.connection as con:
            df = pd.read_sql_query(
                f'''
                SELECT bars{self.sample}.datetime_at, open, high, low, close, gap_size
                FROM bars{self.sample}
                LEFT JOIN gaps_TEST ON gaps_TEST.datetime_at = bars{self.sample}.datetime_at
                WHERE bars{self.sample}.symbol = (?)
                AND ((CAST(strftime('%H', bars{self.sample}.datetime_at) as decimals) >= 9 AND CAST(strftime('%M', bars{self.sample}.datetime_at) as decimals) >= 30) OR CAST(strftime('%H', bars{self.sample}.datetime_at) as decimals) >= 10)
                AND CAST(strftime('%H', bars{self.sample}.datetime_at) as decimals) < 16
                AND CAST(strftime('%w', bars{self.sample}.datetime_at) as decimals) < 5
                AND bars{self.sample}.datetime_at BETWEEN (?) AND (?)
                ''',
                con,
                params=(symbol, start_at, end_at),
                parse_dates='datetime_at',
                # index_col='datetime_at',
            )
        return df

    def load_bars_to_df_in_range(self, symbol, start_at, end_at):  # inclusive - non-inclusive
        with self.connection as con:
            df = pd.read_sql_query(
                f'''
                SELECT datetime_at, open, high, low, close
                FROM bars{self.sample}
                WHERE symbol = (?)
                AND datetime_at BETWEEN (?) AND (?)
                ''',
                con,
                params=(symbol, start_at, end_at)
            )
        return df

    def get_all_bars_symbols(self):
        with self.connection as con:
            df = pd.read_sql_query(
                f'''
                SELECT DISTINCT(symbol)
                FROM bars{self.sample}
                ''',
                con,
            )
        return list(df.symbol)

    def get_market_hours_bars(self, symbol) -> pd.DataFrame:
        with self.connection as con:
            df = pd.read_sql_query(
                f'''
                SELECT datetime_at, open, high, low, close
                FROM bars{self.sample}
                WHERE ((CAST(strftime('%H', bars{self.sample}.datetime_at) as decimals) >= 9 AND CAST(strftime('%M', bars{self.sample}.datetime_at) as decimals) >= 30) OR CAST(strftime('%H', bars{self.sample}.datetime_at) as decimals) >= 10)
                AND CAST(strftime('%H', bars{self.sample}.datetime_at) as decimals) < 16
                AND CAST(strftime('%w', bars{self.sample}.datetime_at) as decimals) < 5
                AND symbol = (?)
                ''',
                con,
                params=(symbol,)
            )
        return df

    def get_all_gaps(self, min_gap_size: float) -> pd.DataFrame:
        with self.connection as con:
            df = pd.read_sql_query(
                f'''
                SELECT gap_size
                FROM gaps{self.sample}
                WHERE timeframe = (?) AND abs(gap_size) >= (?)
                ''',
                con,
                params=(self.timeframe, min_gap_size)
            )
        return df

    def get_gaps_per_symbol(self, min_gap_size: float) -> dict:
        with self.connection as con:
            df = pd.read_sql_query(
                f'''
                SELECT datetime_at, gap_size, symbol
                FROM gaps{self.sample}
                WHERE timeframe = (?) AND abs(gap_size) >= (?)
                ''',
                con,
                params=(self.timeframe, min_gap_size)
            )
        gaps_per_symbol = defaultdict(list)
        for gap in df.to_dict('records'):
            symbol = gap.pop('symbol')
            gaps_per_symbol[symbol].append(gap)
        return gaps_per_symbol

    def save_charts_nth_price(self, charts_df):
        # todo clean table
        self.execute_many_query(
            f'''
                INSERT INTO charts{self.sample} (symbol, timeframe, datetime_at, filepath, n)
                VALUES (?,?,?,?,?)
            ''',
            [(bar.symbol, bar.timeframe, bar.datetime_at, bar.open, bar.high, bar.low, bar.close, bar.volume) for bar in
             charts_df],
        )

    def get_charts_nth_price(self, n_after_bars: int):
        with self.connection as con:
            df = pd.read_sql_query(
                f'''
                SELECT *
                FROM charts{self.sample}
                WHERE after_bars = (?)
                ''',
                con,
                params=(n_after_bars,),
                index_col='id',
            )
        return df

    def get_charts_news_nth_price(self, n_after_bars: int):
        with self.connection as con:
            df = pd.read_sql_query(
                f'''
                SELECT *
                FROM charts_news{self.sample}
                WHERE after_bars = (?)
                ''',
                con,
                params=(n_after_bars,),
                index_col='id',
            )
        return df
