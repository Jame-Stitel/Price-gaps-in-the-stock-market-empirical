from datetime import datetime
from pathlib import Path

from pandas import DataFrame

from src.repository import BarsRepo
from src.utils.candlestick_chart import save_chart


def save_snapshot_for_gap(gap, bars_df: DataFrame, bars_before: int, bars_after: int, path: str):
    gap_index = bars_df[bars_df['datetime_at'] == gap['datetime_at']].index.tolist()[0]
    df_snapshot = bars_df.iloc[max(gap_index - bars_before, 0):(gap_index + bars_after + 1)]
    save_chart(df_snapshot, path)


def save_chart_images_with_gaps(bars_repo: BarsRepo, bars_before: int, bars_after: int, min_gap_size: float):
    gaps_per_symbol = bars_repo.get_gaps_per_symbol(min_gap_size)
    for symbol, gaps in gaps_per_symbol.items():
        print(f'processing: {symbol}')
        folder_path = f'{bars_repo.chart_images_base_path}/{bars_repo.timeframe}/{symbol}'
        Path(folder_path).mkdir(parents=True, exist_ok=True)
        bars_df = bars_repo.get_market_hours_bars(symbol)
        for gap in gaps:
            path = f'{folder_path}/{int(datetime.fromisoformat(gap["datetime_at"]).timestamp())}_{bars_before}_{bars_after}'
            save_snapshot_for_gap(gap, bars_df, bars_before, bars_after, path)
