import json
import os
from datetime import datetime
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
import pytz
from matplotlib import pyplot as plt
from pandas import DataFrame
from pandas.tseries.offsets import BDay

from src.repository import BarsRepo, DbLocation, DbSample, NewsRepo

image_dir = Path('data/charts/5min')


def get_nth_price_for_gap(bars_df, gap_timestamp, n, after_bars=0):
    gap_dt = f'{datetime.utcfromtimestamp(gap_timestamp).strftime("%Y-%m-%d %H:%M:%S")}+00:00'
    gap_dt_after = f'{(datetime.utcfromtimestamp(gap_timestamp) + pd.Timedelta(f"{5 * after_bars} minutes")).strftime("%Y-%m-%d %H:%M:%S")}+00:00'

    try:
        gap_index = bars_df[bars_df['datetime_at'] == gap_dt_after].index.to_list()[0]
    except (ValueError, IndexError) as e:
        gap_index = bars_df[bars_df['datetime_at'] == gap_dt].index.to_list()[0] + 5

    try:
        gap_close = bars_df.iloc[gap_index]['close']
        n_th_close = bars_df.iloc[gap_index + n: gap_index + n + 1]['close'].item()
    except ValueError as e:
        return 0
    return (n_th_close - gap_close) / gap_close


def get_nth_prices_for_gap(bars_df, gap_timestamp, n_list, after_bars=0):
    if after_bars:
        n_list = n_list[:-after_bars]

    n_prices = {int(n): get_nth_price_for_gap(bars_df, gap_timestamp, n, after_bars) for n in n_list}
    return json.dumps(n_prices)


def insert_close_prices_for_nth_bars_after_gap(bars_repo, df_per_symbol, n_list: List[int]):
    symbol = df_per_symbol['symbol'].iloc[0]
    bars_df = bars_repo.get_market_hours_bars(symbol)

    df_per_symbol['n'] = df_per_symbol.apply(lambda row: get_nth_prices_for_gap(bars_df, row['timestamp'], n_list), axis=1)


def insert_close_prices_for_nth_bars_for_after_bars(bars_repo, df_per_symbol, n_list: List[int]):
    symbol = df_per_symbol['symbol'].iloc[0]
    bars_df = bars_repo.get_market_hours_bars(symbol)

    for after_bars in df_per_symbol['after_bars'].unique().tolist():
        df_per_symbol[f'n_after_{after_bars}'] = df_per_symbol.apply(lambda row: get_nth_prices_for_gap(bars_df, row['timestamp'], n_list, after_bars), axis=1)


def save_chart_nth_price(bars_repo: BarsRepo, charts_df):
    with bars_repo.connection as con:
        charts_df.to_sql(name=f'charts{bars_repo.sample}', con=con, if_exists='append', index=False)  # append/replace


def save_chart_nth_price_with_news(bars_repo: BarsRepo, charts_df):
    with bars_repo.connection as con:
        charts_df.to_sql(name=f'charts_news{bars_repo.sample}', con=con, if_exists='append', index=False)  # append/replace


def create_chart_nth_price_db(bars_repo: BarsRepo, n_for_prices: List[int]):
    file_paths = pd.Series(list(image_dir.glob(r'**/*.png')), name='filepath').astype(str)

    symbols = pd.Series(file_paths.apply(lambda x: os.path.split(os.path.split(x)[0])[1]), name='symbol')
    timestamps = pd.Series(file_paths.apply(lambda x: os.path.split(x)[1].split('_')[0]), name='timestamp').astype(np.int)
    before_bars = pd.Series(file_paths.apply(lambda x: os.path.split(x)[1].split('_')[1]), name='before_bars').astype(np.int)
    after_bars = pd.Series(file_paths.apply(lambda x: os.path.split(x)[1].split('_')[2].removesuffix('.png')), name='after_bars').astype(np.int)

    charts_df = pd.concat([timestamps, file_paths, symbols, before_bars, after_bars], axis=1).sample(frac=1.0, random_state=1).reset_index(drop=True)
    charts_df['timeframe'] = '5min'
    charts_dfs_symbol = [x for _, x in charts_df.groupby(charts_df['symbol'])]

    for df in charts_dfs_symbol:
        insert_close_prices_for_nth_bars_after_gap(bars_repo, df, n_for_prices)
        save_chart_nth_price(bars_repo, df)

    # charts_df = pd.concat(charts_dfs_symbol[2:3])  # .sample(frac=1.0, random_state=1).reset_index(drop=True)  # to randomize df
    # save_chart_nth_price(bars_repo, charts_df)


def get_news_for_gap(row, all_news_sentiment: DataFrame):
    news_per_symbol = all_news_sentiment[all_news_sentiment['symbol'] == row['symbol']]
    gap_at = datetime.fromtimestamp(row['timestamp'], tz=pytz.utc)
    if gap_at.time() <= datetime(2000, 1, 1, 9, 45).time():  # 9:30 is market open
        previous_business_day = gap_at - BDay(1)
        news_from = previous_business_day.replace(hour=16, minute=1)  # 16:00 is market close
    else:
        news_from = gap_at - pd.Timedelta('15 minutes')
    news_to = gap_at + pd.Timedelta('5 minutes')
    return news_per_symbol[(news_per_symbol['datetime_at'] <= news_to) & (news_per_symbol['datetime_at'] >= news_from)]


def get_news_count_for_gap(row, all_news_sentiment: DataFrame) -> int:
    return len(get_news_for_gap(row, all_news_sentiment))


def get_news_sentiment_for_gap(row, all_news_sentiment: DataFrame) -> float:
    news = get_news_for_gap(row, all_news_sentiment)
    if news.empty:
        return 0

    scores = news['headline_sentiment'].to_list()  # only headlines
    return sum(scores) / len(scores)


def create_chart_nth_price_with_news_db(bars_repo: BarsRepo, news_repo: NewsRepo, n_for_prices: List[int]):
    file_paths = pd.Series(list(image_dir.glob(r'**/*.png')), name='filepath').astype(str)

    symbols = pd.Series(file_paths.apply(lambda x: os.path.split(os.path.split(x)[0])[1]), name='symbol')
    timestamps = pd.Series(file_paths.apply(lambda x: os.path.split(x)[1].split('_')[0]), name='timestamp').astype(np.int)
    before_bars = pd.Series(file_paths.apply(lambda x: os.path.split(x)[1].split('_')[1]), name='before_bars').astype(np.int)
    after_bars = pd.Series(file_paths.apply(lambda x: os.path.split(x)[1].split('_')[2].removesuffix('.png')), name='after_bars').astype(np.int)

    charts_df = pd.concat([timestamps, file_paths, symbols, before_bars, after_bars], axis=1).sample(frac=1.0, random_state=1).reset_index(drop=True)
    charts_df['timeframe'] = '5min'

    all_news_sentiment = news_repo.get_all_news_sentiment()
    all_news_sentiment['datetime_at'] = pd.to_datetime(all_news_sentiment['datetime_at'])
    charts_df['news_count'] = charts_df.apply(lambda row: get_news_count_for_gap(row, all_news_sentiment), axis=1)
    charts_df['news_sentiment'] = charts_df.apply(lambda row: get_news_sentiment_for_gap(row, all_news_sentiment), axis=1)

    charts_dfs_symbol = [x for _, x in charts_df.groupby(charts_df['symbol'])]

    for df in charts_dfs_symbol:
        insert_close_prices_for_nth_bars_for_after_bars(bars_repo, df, n_for_prices)
        save_chart_nth_price_with_news(bars_repo, df)


def save_biggest_moves_after_gap(bars_repo: BarsRepo, min_gap_size: float):
    gaps_per_symbol = bars_repo.get_gaps_per_symbol(min_gap_size)
    for symbol, gaps in gaps_per_symbol.items():
        print(symbol)
        n_biggest_12 = []
        n_biggest_24 = []
        n_biggest_36 = []
        n_biggest_48 = []

        bars_df = bars_repo.get_market_hours_bars(symbol)
        for gap in gaps:
            gap_timestamp = int(datetime.fromisoformat(gap["datetime_at"]).timestamp())

            n_prices_12 = {int(n): abs(get_nth_price_for_gap(bars_df, gap_timestamp, n)) for n in range(1, 13)}
            n_max_1 = max(n_prices_12, key=n_prices_12.get)
            del n_prices_12[n_max_1]
            n_max_2 = max(n_prices_12, key=n_prices_12.get)
            del n_prices_12[n_max_2]
            n_max_3 = max(n_prices_12, key=n_prices_12.get)
            n_biggest_12.extend([n_max_1, n_max_2, n_max_3])

            n_prices_24 = {int(n): abs(get_nth_price_for_gap(bars_df, gap_timestamp, n)) for n in range(1, 25)}
            n_max_1 = max(n_prices_24, key=n_prices_24.get)
            del n_prices_24[n_max_1]
            n_max_2 = max(n_prices_24, key=n_prices_24.get)
            del n_prices_24[n_max_2]
            n_max_3 = max(n_prices_24, key=n_prices_24.get)
            n_biggest_24.extend([n_max_1, n_max_2, n_max_3])

            n_prices_36 = {int(n): abs(get_nth_price_for_gap(bars_df, gap_timestamp, n)) for n in range(1, 37)}
            n_max_1 = max(n_prices_36, key=n_prices_36.get)
            del n_prices_36[n_max_1]
            n_max_2 = max(n_prices_36, key=n_prices_36.get)
            del n_prices_36[n_max_2]
            n_max_3 = max(n_prices_36, key=n_prices_36.get)
            n_biggest_36.extend([n_max_1, n_max_2, n_max_3])

            n_prices_48 = {int(n): abs(get_nth_price_for_gap(bars_df, gap_timestamp, n)) for n in range(1, 49)}
            n_max_1 = max(n_prices_48, key=n_prices_48.get)
            del n_prices_48[n_max_1]
            n_max_2 = max(n_prices_48, key=n_prices_48.get)
            del n_prices_48[n_max_2]
            n_max_3 = max(n_prices_48, key=n_prices_48.get)
            n_biggest_48.extend([n_max_1, n_max_2, n_max_3])

        with open('../data/biggest_moves/n_biggest_1-12_NEW.json', 'a') as f_12:
            f_12.write(f'{", ".join(map(str, n_biggest_12))}, ')
        with open('../data/biggest_moves/n_biggest_1-24_NEW.json', 'a') as f_24:
            f_24.write(f'{", ".join(map(str, n_biggest_24))}, ')
        with open('../data/biggest_moves/n_biggest_1-36_NEW.json', 'a') as f_36:
            f_36.write(f'{", ".join(map(str, n_biggest_36))}, ')
        with open('../data/biggest_moves/n_biggest_1-48_NEW.json', 'a') as f_48:
            f_48.write(f'{", ".join(map(str, n_biggest_48))}, ')


def biggest_moves_after_gap_histogram(biggest_moves_file):
    n_biggest = []
    if not isinstance(biggest_moves_file, list):
        biggest_moves_file = [biggest_moves_file]

    for file in biggest_moves_file:
        with open(file, 'r') as f:
            n_biggest.extend(json.load(f))

    plt.figure(figsize=[21, 14])
    labels, counts = np.unique(n_biggest, return_counts=True)
    plt.bar(labels, counts, align='center', width=1, color='#333333')
    plt.gca().set_xticks(labels)
    plt.grid()
    plt.xticks(fontsize=17)
    plt.yticks(fontsize=17)
    plt.ylabel('Count of occurrences', fontsize=22)
    plt.xlabel('Nth candlestick after a gap with the biggest price difference', fontsize=22)
    plt.show()


if __name__ == '__main__':
    bars_repo = BarsRepo(DbLocation.LOCAL, DbSample.ALL)
    # save_biggest_moves_after_gap(bars_repo, 0.03)

    moves_file = '../data/biggest_moves/n_biggest_1-48_NEW.json'
    moves_file = ['../data/biggest_moves/n_biggest_1-12.json', '../data/biggest_moves/n_biggest_1-24.json',
                  '../data/biggest_moves/n_biggest_1-36.json', '../data/biggest_moves/n_biggest_1-48.json']
    biggest_moves_after_gap_histogram(moves_file)
