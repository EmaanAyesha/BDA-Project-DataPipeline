from pymongo import MongoClient
import pandas as pd

def load_to_mongodb():
    print("📦 Loading data into MongoDB...")
    df = pd.read_parquet("data/processed/sentiment_reviews.parquet")
    
    client = MongoClient("mongodb://localhost:27017/")
    db = client["amazon_reviews"]
    collection = db["sentiment_data"]

    data = df.to_dict(orient="records")
    collection.delete_many({})  # clear previous data
    collection.insert_many(data)
    print(f"✅ Inserted {len(data)} records into MongoDB")

if __name__ == "__main__":
    load_to_mongodb()
