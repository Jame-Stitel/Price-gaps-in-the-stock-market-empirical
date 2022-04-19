from datetime import date

import requests

from src.utils.sp500 import get_symbols


class StockNewsAPIClient:
    TOKEN = 'xxx'
    # URL = 'https://stocknewsapi.com/api/v1/stat'  # sentiment analysis
    URL = 'https://stocknewsapi.com/api/v1/'  # news

    @classmethod
    def get_headlines(cls, symbol: str):
        params = {
            'token': cls.TOKEN,
            'tickers': symbol,
            'date': '03012019-04032021',  # 1/3/2019 - 3/4/2021
            'extra-fields': 'id,eventid,rankscore',
            'page': 1,  # min 1 max total_pages
            'items': 50
        }

        response = requests.get(cls.URL, params=params)
        data = response.json()
        total_pages = data['total_pages']

        for date, values in data['clients'].items():  # 2021-04-01
            pass # {'AMZN': {'Neutral': 2, 'Positive': 5, 'Negative': 2, 'sentiment_score': 0.722}}

    @classmethod
    def download_all(cls, symbol: str, start_date: date = date(2020, 1, 1), end_date: date = date(2021, 4, 3)):  # end not inclusive
        headlines = cls.get_headlines(symbol)
        # NewsRepo.add_headlines(headlines, symbol)
        # print(f'{symbol} _ saved: {len(headlines)} for date: {dt}')


if __name__ == '__main__':
    symbols = get_symbols()
    for symbol in symbols:
        StockNewsAPIClient.download_all(symbol)



"""
AMZN
2021-04-01: {'Neutral': 2, 'Positive': 5, 'Negative': 2, 'sentiment_score': 0.722}
"""
