from datetime import date, timedelta

import requests

from src.repository.news_repo import NewsRepo


class XigniteClient:
    TOKEN = 'xxx'
    URL = 'https://globalnews.xignite.com/xGlobalNews.json/GetHistoricalSecurityHeadlines'

    @classmethod
    def get_headlines(cls, symbol: str, from_date: date, to_date: date):
        params = {
            '_Token': cls.TOKEN,
            'IdentifierType': 'Symbol',
            'Identifier': symbol,
            'StartDate': from_date.strftime("%m/%d/%Y"),
            'EndDate': to_date.strftime("%m/%d/%Y")
        }

        response = requests.get(cls.URL, params=params)
        data = response.json()
        if data.get('Outcome') == 'RegistrationError':
            print(data)
            raise Exception
        return data

    @classmethod
    def download_all(cls, symbol: str, start_date: date = date(2020, 1, 1), end_date: date = date(2021, 4, 3)):  # end not inclusive
        for n in range(int((end_date - start_date).days)):
            dt = start_date + timedelta(n)

            headlines = cls.get_headlines(symbol, from_date=dt, to_date=dt)
            # NewsRepo.add_headlines(headlines, symbol)
            print(f'{symbol} _ saved: {len(headlines)} for date: {dt}')
