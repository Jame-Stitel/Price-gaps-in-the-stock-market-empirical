import requests


# https://api.twelvedata.com/time_series?symbol=AAPL&interval=1min&apikey=demo&source=docs

class TwelveDataAPIClient:
    TOKEN = 'xxx'
    URL = 'https://api.twelvedata.com/'

    @classmethod
    def get_prices(cls, symbol: str):
        params = {
            'apikey': cls.TOKEN,
            'symbol': symbol,
            'interval': '5min',
            'source': 'docs',
            'start_date': '2016-01-01',
            'end_date': '2016-01-06'
        }

        response = requests.get(f'{cls.URL}earliest_timestamp', params={
            'apikey': cls.TOKEN,
            'symbol': symbol,
            'interval': '1h',
        })
        data = response.json()

        response = requests.get(f'{cls.URL}time_series', params=params)
        data = response.json()
        print(data['values'])


if __name__ == '__main__':
    TwelveDataAPIClient.get_prices('AAPL')
