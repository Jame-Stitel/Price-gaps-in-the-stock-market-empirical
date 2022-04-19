from datetime import datetime

from alpaca_trade_api.entity_v2 import NewsV2

from src.models.base import CamelAliasModel


class NewsDb(CamelAliasModel):
    symbol: str
    datetime_at: datetime
    author: str
    content: str
    headline: str
    source: str
    summary: str
    url: str

    @classmethod
    def create_from(cls, news_one: NewsV2, symbol):
        try:
            content = news_one.content
        except AttributeError:
            content = ''

        return cls(
            symbol=symbol,
            datetime_at=news_one.created_at.tz_convert('UTC').to_pydatetime(),
            author=news_one.author,
            content=content,
            headline=news_one.headline,
            source=news_one.source,
            summary=news_one.summary,
            url=news_one.url,
        )
