from fastapi import FastAPI
from pymongo import MongoClient
from typing import List

app = FastAPI(title="Amazon Reviews Sentiment API")

# MongoDB Connection
client = MongoClient("mongodb://localhost:27017/")
db = client["amazon_reviews"]
collection = db["sentiment_data"]

@app.get("/")
def home():
    return {"message": "Welcome to Amazon Reviews Sentiment API"}

@app.get("/reviews/positive")
def get_positive_reviews(limit: int = 10):
    reviews = list(collection.find({"sentiment": "POSITIVE"}, {"_id": 0, "reviews_text": 1}).limit(limit))
    return {"count": len(reviews), "positive_reviews": reviews}

@app.get("/reviews/negative")
def get_negative_reviews(limit: int = 10):
    reviews = list(collection.find({"sentiment": "NEGATIVE"}, {"_id": 0, "reviews_text": 1}).limit(limit))
    return {"count": len(reviews), "negative_reviews": reviews}

@app.get("/summary")
def sentiment_summary():
    positive = collection.count_documents({"sentiment": "POSITIVE"})
    negative = collection.count_documents({"sentiment": "NEGATIVE"})
    total = positive + negative
    return {
        "total_reviews": total,
        "positive": positive,
        "negative": negative,
        "positive_percent": round((positive / total) * 100, 2),
        "negative_percent": round((negative / total) * 100, 2)
    }
