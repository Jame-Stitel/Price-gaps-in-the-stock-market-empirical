from typing import List

import pandas as pd

from src.models.repository.news import NewsDb
from src.repository.base import DatabaseClient


class NewsRepo(DatabaseClient):
    def add_news(self, news: List[NewsDb]):
        self.execute_many_query(
            f'''
            INSERT INTO news{self.sample} (symbol, datetime_at, author, content, headline, source, summary, url)
            VALUES (?,?,?,?,?,?,?,?)
        ''',
            [(n.symbol, n.datetime_at, n.author, n.content, n.headline, n.source, n.summary, n.url) for n in news],
        )

    def get_all_news(self) -> pd.DataFrame:
        with self.connection as con:
            df = pd.read_sql_query(
                f'''
                SELECT id, symbol, datetime_at, headline, summary, content
                FROM news{self.sample}
                ''',
                con,
            )
        return df

    def get_all_news_sentiment(self) -> pd.DataFrame:
        with self.connection as con:
            df = pd.read_sql_query(
                f'''
                SELECT symbol, datetime_at, headline_sentiment, summary_sentiment
                FROM news_sentiment{self.sample}
                ''',
                con,
            )
        return df
