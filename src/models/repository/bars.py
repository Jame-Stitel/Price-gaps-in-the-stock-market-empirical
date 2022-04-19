from datetime import datetime

from alpaca_trade_api.entity import Bar

from src.models.base import CamelAliasModel


class BarDB(CamelAliasModel):
    symbol: str
    timeframe: str
    datetime_at: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

    @classmethod
    def create_from(cls, bar: Bar, symbol, timeframe):
        return cls(
            symbol=symbol,
            timeframe=timeframe,
            datetime_at=bar.t.tz_convert('UTC').to_pydatetime(),
            open=bar.o,
            high=bar.h,
            low=bar.l,
            close=bar.c,
            volume=bar.v,
        )
