
from pyspark.sql import SparkSession

def ingest_data():
    spark = SparkSession.builder.appName("AmazonReviewIngestion").getOrCreate()
    df = spark.read.csv("data/Datafiniti_Amazon_Consumer_Reviews_of_Amazon_Products.csv", header=True, inferSchema=True)
    df.write.mode("overwrite").parquet("data/amazon_reviews_raw.parquet")
    print("Data ingestion complete and saved as Parquet.")
    spark.stop()

if __name__ == "__main__":
    ingest_data()
