import pandas as pd
from textblob import TextBlob

def run_sentiment_analysis():
    print("Loading processed data...")
    df = pd.read_parquet("data/processed/cleaned_reviews.parquet")

    print("Running sentiment analysis using TextBlob...")
    df["polarity"] = df["reviews_text"].apply(lambda x: TextBlob(x).sentiment.polarity)
    df["sentiment"] = df["polarity"].apply(
        lambda x: "POSITIVE" if x > 0 else ("NEGATIVE" if x < 0 else "NEUTRAL")
    )

    print("Sentiment analysis complete!")
    print(df[["reviews_text", "sentiment"]].head())

    df.to_parquet("data/processed/sentiment_reviews.parquet", index=False)
    print("Saved sentiment results to data/processed/sentiment_reviews.parquet")

if __name__ == "__main__":
    run_sentiment_analysis()
