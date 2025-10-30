
import pandas as pd
from pymongo import MongoClient

def load_to_mongodb():
    df = pd.read_parquet("data/processed/sentiment_reviews.parquet")
    client = MongoClient("mongodb://localhost:27017/")
    db = client["amazon_reviews"]
    collection = db["review_sentiments"]
    collection.delete_many({})
    collection.insert_many(df.to_dict("records"))
    print("Data successfully loaded into MongoDB!")

if __name__ == "__main__":
    load_to_mongodb()
