from typing import List

from matplotlib import pyplot as plt
from pandas import DataFrame

from src.repository import BarsRepo


def scan_gaps(df_bars: DataFrame, min_gap_size: float) -> DataFrame:
    df = df_bars.copy()
    # df[f'gap_down_{gap_size}'] = df.open < df.close.shift(1) * (1-gap_size)
    # df[f'gap_up_{gap_size}'] = df.open > df.close.shift(1) * (1+gap_size)
    df['is_gap'] = (df.open >= df.close.shift(1) * (1 + min_gap_size)) | (df.open <= df.close.shift(1) * (1 - min_gap_size))
    df['gap_size'] = (df.open - df.close.shift(1)) / df.close.shift(1)

    df = df[df['is_gap'] == True]
    df.drop(['open', 'high', 'low', 'close', 'is_gap'], axis=1, inplace=True)
    return df


def save_gaps(df_gaps: DataFrame, symbol: str, bars_repo: BarsRepo):
    df_gaps['symbol'] = symbol
    df_gaps['timeframe'] = BarsRepo.timeframe

    with bars_repo.connection as con:
        df_gaps.to_sql(name=f'gaps{bars_repo.sample}', con=con, if_exists='append', index=False)  # append/replace


def gaps_histogram(bars_repo: BarsRepo):
    gaps = bars_repo.get_all_gaps(min_gap_size=0.01)
    chart = gaps.plot(kind='hist', bins='fd', range=(-0.1, 0.1), figsize=(21,14), legend=False, grid=True, fontsize=17, color='#333333')
    chart.set_ylabel('Count of gaps', fontsize=22)
    chart.set_xlabel('Size of gaps', fontsize=22)
    chart.set_title('Frequency of gaps per their size', fontsize=26)
    chart.set_xticks([-.1, -.07, -.05, -.03, -.02, -.01, .01, .02, .03, .05, .07, .1])
    chart.set_xticklabels([-.1, -.07, -.05, -.03, -.02, -.01, .01, .02, .03, .05, .07, .1])
    plt.show()
    plt.savefig('price_gaps_frequency.png')


def find_and_save_gaps(bars_repo: BarsRepo, min_gap_size: float, symbols: List[str] = None):
    if not symbols:
        symbols = bars_repo.get_all_bars_symbols()

    for symbol in symbols:
        bars_db = bars_repo.get_market_hours_bars(symbol)
        gaps_df = scan_gaps(bars_db, min_gap_size)
        save_gaps(gaps_df, symbol, bars_repo)
