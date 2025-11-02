import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from wordcloud import WordCloud

# ============================================================
# MAIN VISUALIZATION FUNCTION
# ============================================================

def visualize_sentiment():
    print("📊 Loading sentiment data...")
    df = pd.read_parquet("data/processed/sentiment_reviews.parquet")

    # 🧹 Standardize column names if necessary
    df.columns = df.columns.str.lower()

    # Rename common variants for consistency
    if 'reviews_text' in df.columns:
        df.rename(columns={'reviews_text': 'review_text'}, inplace=True)

    # --- SENTIMENT DISTRIBUTION ---
    sentiment_counts = df["sentiment"].value_counts()
    print("\nSentiment distribution:\n", sentiment_counts)

    # --- 1️⃣ PIE CHART ---
    plt.figure(figsize=(6, 6))
    sentiment_counts.plot.pie(autopct='%1.1f%%', startangle=90, colors=["#66bb6a", "#ef5350"])
    plt.title("Sentiment Distribution of Amazon Reviews")
    plt.ylabel("")
    plt.tight_layout()
    plt.savefig("data/sentiment_distribution.png")
    plt.close()
    print("✅ Pie chart saved as 'data/sentiment_distribution.png'")

    # --- 2️⃣ BAR CHART ---
    sns.countplot(x="sentiment", data=df, palette="Set2")
    plt.title("Sentiment Frequency Count")
    plt.xlabel("Sentiment")
    plt.ylabel("Number of Reviews")
    plt.tight_layout()
    plt.savefig("data/sentiment_chart.png")
    plt.close()
    print("✅ Bar chart saved as 'data/sentiment_chart.png'")

    # --- 3️⃣ INTERACTIVE PLOTLY VISUALIZATION ---
    fig = px.histogram(df, x="sentiment", title="Interactive Sentiment Distribution", color="sentiment")
    fig.write_html("data/sentiment_trends.html")
    print("✅ Interactive chart saved as 'data/sentiment_trends.html'")

    # --- 4️⃣ SENTIMENT TREND OVER TIME ---
    if 'review_date' in df.columns:
        df['review_date'] = pd.to_datetime(df['review_date'], errors='coerce')
        trend = df.groupby(df['review_date'].dt.to_period('M'))['sentiment'].value_counts().unstack().fillna(0)
        trend.plot(kind='line', figsize=(10,6))
        plt.title("Sentiment Trend Over Time")
        plt.xlabel("Month")
        plt.ylabel("Number of Reviews")
        plt.tight_layout()
        plt.savefig("data/sentiment_trend.png")
        plt.close()
        print("✅ Sentiment trend chart saved as 'data/sentiment_trend.png'")

    # --- 5️⃣ WORDCLOUDS FOR POSITIVE/NEGATIVE REVIEWS ---
    pos_text = " ".join(df[df['sentiment'].str.lower()=="positive"]['review_text'].dropna().tolist())
    neg_text = " ".join(df[df['sentiment'].str.lower()=="negative"]['review_text'].dropna().tolist())
    
    wordcloud_pos = WordCloud(width=800, height=400, background_color='white').generate(pos_text)
    wordcloud_neg = WordCloud(width=800, height=400, background_color='black').generate(neg_text)
    wordcloud_pos.to_file("data/positive_wordcloud.png")
    wordcloud_neg.to_file("data/negative_wordcloud.png")
    print("✅ Wordclouds saved as 'data/positive_wordcloud.png' and 'data/negative_wordcloud.png'")

    # --- 6️⃣ AVERAGE RATING VS SENTIMENT ---
    if 'rating' in df.columns:
        avg_rating = df.groupby('sentiment')['rating'].mean()
        avg_rating.plot(kind='bar', color=['#ff6666','#66b3ff','#99ff99'])
        plt.title("Average Rating per Sentiment Category")
        plt.ylabel("Average Rating")
        plt.tight_layout()
        plt.savefig("data/avg_rating_vs_sentiment.png")
        plt.close()
        print("✅ Avg rating vs sentiment chart saved as 'data/avg_rating_vs_sentiment.png'")

    # --- 7️⃣ TOP 10 CATEGORIES BY SENTIMENT ---
    if 'category' in df.columns:
        category_sentiment = df.groupby(['category','sentiment']).size().unstack(fill_value=0)
        top_categories = category_sentiment.sum(axis=1).nlargest(10).index
        category_sentiment.loc[top_categories].plot(kind='bar', stacked=True, figsize=(10,6))
        plt.title("Top 10 Categories by Sentiment")
        plt.xlabel("Product Category")
        plt.ylabel("Number of Reviews")
        plt.tight_layout()
        plt.savefig("data/top_categories_sentiment.png")
        plt.close()
        print("✅ Top categories chart saved as 'data/top_categories_sentiment.png'")

    # --- 8️⃣ SAMPLE REVIEWS ---
    print("\n🟢 Sample Positive Review:")
    print(df[df["sentiment"].str.lower() == "positive"]["review_text"].iloc[0])
    print("\n🔴 Sample Negative Review:")
    print(df[df["sentiment"].str.lower() == "negative"]["review_text"].iloc[0])

# ============================================================
# MAIN EXECUTION
# ============================================================

if __name__ == "__main__":
    visualize_sentiment()
