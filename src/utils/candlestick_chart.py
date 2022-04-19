import mplfinance as mpf
import pandas as pd


def show_candlestick(df):
    df_for_plot = df.set_index('datetime_at')
    mpf.plot(df_for_plot, type='candle',
             # axisoff=True,
             ylabel='',
             tight_layout=True,
             # figsize=(8.00*100,5.75*100),
             # figscale=5, # 7
             show_nontrading=False,
             # savefig='test-mplfiance.png'
    )


def save_chart(df, path):
    df_for_plot = df.set_index(pd.DatetimeIndex(df['datetime_at']))
    mpf.plot(df_for_plot, type='candle',
             axisoff=True,
             ylabel='',
             tight_layout=True,
             figscale=1,  # 7
             show_nontrading=False,
             savefig=path,
    )
