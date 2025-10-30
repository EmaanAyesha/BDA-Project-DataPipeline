from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length

def process_data():
    spark = SparkSession.builder.appName("AmazonReviewProcessing").getOrCreate()

    try:
        print("Loading raw data...")
        df = spark.read.option("header", True).csv("data/amazon_reviews.csv")

        print(" Data loaded successfully!")
        df.printSchema()

        # Fix: Rename columns with '.' to '_'
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.replace('.', '_'))

        print("Columns renamed for Spark compatibility!")

        # Select only useful columns
        cleaned_df = df.select(
            "id",
            "name",
            "brand",
            "categories",
            "reviews_text",
            "reviews_rating"
        ).dropna()

        print("Data cleaned successfully!")
        print(f"Total rows after cleaning: {cleaned_df.count()}")

        # Save cleaned data
        cleaned_df.write.mode("overwrite").parquet("data/processed/cleaned_reviews.parquet")
        print("Processed data saved successfully!")

        cleaned_df.show(5)

    except Exception as e:
        print(f"Error while processing data: {e}")

    finally:
        spark.stop()


if __name__ == "__main__":
    process_data()
