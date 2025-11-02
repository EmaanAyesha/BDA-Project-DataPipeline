import os
import pandas as pd
from transformers import pipeline

def run_sentiment_analysis():
    print("🔍 Loading processed reviews...")
    df = pd.read_parquet("data/processed/cleaned_reviews.parquet")

    print("🤖 Loading Hugging Face model...")
    sentiment_model = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

    print("⚙️ Running sentiment inference...")
    df["sentiment"] = df["reviews_text"].apply(lambda x: sentiment_model(x[:512])[0]["label"])

    output_path = "data/processed/sentiment_reviews.parquet"
    df.to_parquet(output_path, index=False)
    print(f"✅ Sentiment analysis complete. Saved results to {output_path}")

if __name__ == "__main__":
    run_sentiment_analysis()
