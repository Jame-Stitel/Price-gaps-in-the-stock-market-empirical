import time
from datetime import date
from typing import List

from tqdm import tqdm

from src.clients.alpaca_client import alpaca
from src.repository import NewsRepo


def load_state() -> list:
    try:
        with open('already_downloaded_news.csv', newline='') as f:
            data = f.read().splitlines()
    except FileNotFoundError:
        return []
    return data


def save_state(new_done_symbols: list):
    with open('already_downloaded_news.csv', 'a+') as cache_f:
        cache_f.write('\n'.join(new_done_symbols) + '\n')


def download_symbols_news(symbols: List[str], news_repo: NewsRepo):
    already_done = load_state()

    for symbol in tqdm(symbols):
        if symbol not in already_done:
            start = time.time()

            for news_db in alpaca.download_news(symbol, end_at=date(2021, 11, 1), include_content=True):
                news_repo.add_news(news_db)
            save_state([symbol])

            end = time.time()
            print(f'symbol {symbol} finished in: {end - start} seconds')
