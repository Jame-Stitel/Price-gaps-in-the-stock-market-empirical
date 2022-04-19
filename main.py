from typing import Tuple

from src.A1_downloader import download_symbols
from src.A2_news_downloader import download_symbols_news
from src.B1_gap_scanner import find_and_save_gaps, gaps_histogram
from src.B2_news_sentiment import evaluate_and_save_news_sentiment
from src.C_chart_creator import save_chart_images_with_gaps
from src.D_chart_with_nth_price import create_chart_nth_price_db, create_chart_nth_price_with_news_db
from src.repository import DbLocation, DbSample, BarsRepo, NewsRepo
from src.utils.sp500 import get_symbols


def prep_repo(db_location: DbLocation, db_sample: DbSample) -> Tuple[BarsRepo, NewsRepo]:
    bars_repo = BarsRepo(db_location, db_sample)
    bars_repo.create_tables()

    news_repo = NewsRepo(db_location, db_sample)
    news_repo.create_tables()
    return bars_repo, news_repo


def main(db_location, db_sample, bars_before, bars_after, min_gap_size):
    # 0 prep BarsRepo
    bars_repo, news_repo = prep_repo(db_location, db_sample)

    # 1 download S&P prices and news data
    download_symbols(get_symbols(), bars_repo)
    download_symbols_news(get_symbols(), news_repo)

    # 2 find and save gaps
    find_and_save_gaps(bars_repo, min_gap_size)
    gaps_histogram(bars_repo)

    # 2.2 evaluate and save news sentiment
    evaluate_and_save_news_sentiment(news_repo)

    # 3 create charts with gaps
    save_chart_images_with_gaps(bars_repo, bars_before, bars_after, min_gap_size)

    # 4 create db for model
    create_chart_nth_price_db(bars_repo, list(range(1, 37)))  # 24th is 2 hours after gap
    create_chart_nth_price_with_news_db(bars_repo, news_repo, list(range(1, 37)))  # 24th is 2 hours after gap


if __name__ == '__main__':
    # main(DbLocation.LOCAL, DbSample.TEST, bars_before=12*4, bars_after=0)
    main(
        db_location=DbLocation.LOCAL,
        db_sample=DbSample.ALL,
        bars_before=12 * 4,  # == 4 hours
        bars_after=0,  # 5 = 6 including the gap bar == 30 minutes
        min_gap_size=0.03  # in % change open from previous close
    )
