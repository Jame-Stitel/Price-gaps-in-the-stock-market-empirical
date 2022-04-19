from datetime import date

import requests

from src.utils.sp500 import get_symbols


class EODHistoricalDataClient:
    TOKEN = 'xxx'
    URL = 'https://eodhistoricaldata.com/api/news'

    @classmethod
    def get_headlines(cls, symbol: str):
        params = {
            'api_token': cls.TOKEN,
            's': 'AAPL.US',
            # 'from': '2019-01-01',
            # 'to': '2020-04-02',
            # 'offset': 0,
            'limit': 1000,
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
        EODHistoricalDataClient.download_all(symbol)
