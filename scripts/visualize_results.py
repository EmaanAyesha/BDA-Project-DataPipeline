
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px

def visualize_results():
    df = pd.read_parquet("data/processed/sentiment_reviews.parquet")
    plt.figure(figsize=(6,4))
    df['sentiment'].value_counts().plot(kind='bar', color=['green','red'])
    plt.title('Sentiment Distribution')
    plt.xlabel('Sentiment')
    plt.ylabel('Count')
    plt.savefig('data/sentiment_distribution.png')
    plt.show()
    fig = px.histogram(df, x='sentiment', title='Sentiment Distribution (Plotly)')
    fig.write_html('data/sentiment_chart.html')
    print("Visualization saved to /data folder.")

if __name__ == "__main__":
    visualize_results()
