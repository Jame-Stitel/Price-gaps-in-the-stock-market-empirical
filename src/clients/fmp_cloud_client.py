from datetime import date, timedelta

import requests


class FMPCloudClient:
    TOKEN = 'xxx'
    URL = 'https://fmpcloud.io/api/v3/stock_news'

    @classmethod
    def get_headlines(cls, from_date: date, to_date: date):
        params = {
            'apikey': cls.TOKEN,
            'from': date(2021, 3, 29).strftime("%Y-%m-%d"),
            'to': date(2021, 3, 30).strftime("%Y-%m-%d"),
        }

        response = requests.get(cls.URL, params=params)
        data = response.json()

        return data

    @classmethod
    def download_all(cls, start_date: date = date(2020, 1, 1), end_date: date = date(2021, 4, 3)):  # end not inclusive
        for n in range(int((end_date - start_date).days)):
            dt = start_date + timedelta(n)
            dt = date(2021, 3, 30)

            headlines = cls.get_headlines(from_date=dt, to_date=dt)
            # NewsRepo.add_headlines(headlines, symbol)
            print(f'saved: {len(headlines)} for date: {dt}')


if __name__ == '__main__':
    FMPCloudClient.download_all()
