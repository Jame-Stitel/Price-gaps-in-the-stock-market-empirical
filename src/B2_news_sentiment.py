import time

from pandas import DataFrame
from transformers import pipeline

from src.repository import NewsRepo

classifier = pipeline('sentiment-analysis')


def evaluate_sentiment_for_text(text: str) -> float:
    text = text.strip()
    if not text:
        return 0
    result = classifier(text)
    if len(result) > 1:
        raise ValueError
    result = result[0]
    if result['label'] == 'POSITIVE':
        return result['score']
    elif result['label'] == 'NEGATIVE':
        return (-1) * result['score']
    else:
        ValueError(result['label'])


def get_sentiment_for_news(df_news: DataFrame) -> DataFrame:
    df = df_news.copy()
    df['news_id'] = df['id']
    t = time.time()
    df['headline_sentiment'] = df.apply(lambda row: evaluate_sentiment_for_text(row['headline']), axis=1)
    print(f'headlines done in: {(time.time() - t)/60} minutes')
    t = time.time()
    df['summary_sentiment'] = df.apply(lambda row: evaluate_sentiment_for_text(row['summary']), axis=1)
    print(f'summaries done in: {(time.time() - t)/60} minutes')
    # t = time.time()
    # df['content_sentiment'] = df.apply(lambda row: evaluate_sentiment_for_text(row['content']), axis=1)
    # print(f'contents done in: {(time.time() - t)/60} minutes')

    df.drop(['headline', 'summary', 'content', 'id'], axis=1, inplace=True)
    return df


def save_news_sentiment(df_news_sentiment: DataFrame, news_repo: NewsRepo):
    with news_repo.connection as con:
        df_news_sentiment.to_sql(name=f'news_sentiment{news_repo.sample}', con=con, if_exists='append', index=False)  # append/replace


def evaluate_and_save_news_sentiment(news_repo: NewsRepo):
    news_df = news_repo.get_all_news()
    news_sentiment_df = get_sentiment_for_news(news_df)
    save_news_sentiment(news_sentiment_df, news_repo)
