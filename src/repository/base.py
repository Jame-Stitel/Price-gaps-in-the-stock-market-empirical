import logging
import os
import sqlite3
from enum import Enum
from sqlite3 import Error


DB_PATH = '/data/repo.db'


class DbLocation(Enum):
    LOCAL = f'{os.getcwd()}{DB_PATH}'


class DbSample(Enum):
    TEST = '_TEST'
    ALL = ''


class DatabaseClient:
    chart_images_base_path = f'{os.getcwd()}/data/charts'

    def __init__(self, location: DbLocation, sample: DbSample):
        self.connection = sqlite3.connect(location.value)
        self.sample = sample.value

    def execute_query(self, query: str, data: tuple = None):
        with self.connection as con:
            cur = con.cursor()

            try:
                if data:
                    cur.execute(query, data)
                else:
                    cur.execute(query)
                con.commit()
                logging.info(f'Query executed successfully, query={query}')
            except Error as e:
                logging.error(f'Query execution failed, error={e}')
        return cur

    def execute_many_query(self, query: str, data: list):
        with self.connection as con:
            cur = con.cursor()

            try:
                cur.executemany(query, data)
                con.commit()
                logging.info(f'Many Query executed successfully, query={query}')
            except Error as e:
                logging.error(f'Many Query execution failed, error={e}')
        return cur

    def execute_read_query(self, query: str, data: tuple = None):
        with self.connection as con:
            cur = con.cursor()

            try:
                if data:
                    cur.execute(query, data)
                else:
                    cur.execute(query)
                result = cur.fetchall()
                logging.info(f'Read Query executed successfully, query={query}')
            except Error as e:
                logging.error(f'Read Query execution failed, error={e}')
        return result

    def create_tables(self):
        self.execute_query(
            f'''
                CREATE TABLE IF NOT EXISTS bars{self.sample} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    datetime_at DATETIME NOT NULL,
                    open FLOAT NOT NULL,
                    high FLOAT NOT NULL,
                    low FLOAT NOT NULL,
                    close FLOAT NOT NULL,
                    volume INTEGER NOT NULL
                );
            '''
        )

        self.execute_query(
            f'''
                CREATE TABLE IF NOT EXISTS gaps{self.sample} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    datetime_at DATETIME NOT NULL,
                    gap_size FLOAT NOT NULL
                );
            '''
        )

        self.execute_query(
            f'''
                CREATE TABLE IF NOT EXISTS charts{self.sample} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timeframe TEXT NOT NULL,
                    timestamp DATETIME NOT NULL,
                    filepath TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    before_bars INTEGER NOT NULL,
                    after_bars INTEGER NOT NULL,
                    n JSON NOT NULL
                );
            '''
        )

        self.execute_query(
            f'''
                CREATE TABLE IF NOT EXISTS charts_news{self.sample} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timeframe TEXT NOT NULL,
                    timestamp DATETIME NOT NULL,
                    filepath TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    before_bars INTEGER NOT NULL,
                    after_bars INTEGER NOT NULL,
                    n_after_0 JSON NOT NULL,
                    n_after_5 JSON NOT NULL,
                    news_count INTEGER NOT NULL,
                    news_sentiment FLOAT NOT NULL
                );
            '''
        )

        self.execute_query(
            f'''
                CREATE TABLE IF NOT EXISTS news{self.sample} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    datetime_at DATETIME NOT NULL,
                    author TEXT NOT NULL,
                    content TEXT NOT NULL,
                    headline TEXT NOT NULL,
                    source TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    url TEXT NOT NULL
                );
            '''
        )
