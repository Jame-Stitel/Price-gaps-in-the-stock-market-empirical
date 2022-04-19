import time
from datetime import date
from typing import List

from tqdm import tqdm

from src.clients.alpaca_client import alpaca
from src.repository import BarsRepo


def load_state() -> list:
    try:
        with open('already_downloaded.csv', newline='') as f:
            data = f.read().splitlines()
    except FileNotFoundError:
        return []
    return data


def save_state(new_done_symbols: list):
    with open('already_downloaded.csv', 'a+') as cache_f:
        cache_f.write('\n'.join(new_done_symbols) + '\n')


def download_symbols(symbols: List[str], bars_repo: BarsRepo):
    already_done = load_state()

    for symbol in tqdm(symbols):
        if symbol not in already_done:
            start = time.time()

            for bars_db in alpaca.download_5_min(symbol, end_at=date(2021, 11, 1)):
                bars_repo.add_bars(bars_db)
            save_state([symbol])

            end = time.time()
            print(f'symbol {symbol} finished in: {end - start} seconds')
