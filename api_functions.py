import requests
import redis
import json
import matplotlib.pyplot as plt
from db_config import get_redis_connection

class Analysis:
    
    # Class to process JSON data fetched from an API and perform various operations.
    

    def __init__(self):
        self.redis_conn = get_redis_connection()

    def fetch_data_from_api(self):
        
        # Fetches JSON data from the API.
        
        api_url = 'https://tradestie.com/api/v1/apps/reddit'
        response = requests.get(api_url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}")

    def insert_into_redis(self, data):
        
        # Inserts new data into Redis after deleting existing data.
        
        self.delete_existing_data()
        for index, item in enumerate(data):
            key = f"Object:{index}"
            try:
                self.redis_conn.execute_command('JSON.SET', key, '.', json.dumps(item))
            except redis.exceptions.ResponseError as e:
                print(f"Error inserting data into Redis: {e}")
                continue
        return len(data)

    def delete_existing_data(self):
        
        # Deletes existing data from Redis with keys starting with 'Object:'.
        
        keys = self.redis_conn.keys("Object:*")
        for key in keys:
            self.redis_conn.delete(key)

    def fetch_data_from_redis(self):
        
        # Fetches data from Redis.
        
        data = []
        try:
            keys = self.redis_conn.keys("Object:*")
            for key in keys:
                raw_data = self.redis_conn.execute_command('JSON.GET', key)
                data.append(json.loads(raw_data))
            return data
        except Exception as e:
            print(f"Error retrieving data from Redis: {e}")
            return None

    def generate_sentiment_chart(self, data):
        
        # Generates a bar chart based on sentiment data.
        
        sentiments = [item['sentiment'] for item in data]
        sentiment_counts = {sentiment: sentiments.count(sentiment) for sentiment in set(sentiments)}

        plt.bar(sentiment_counts.keys(), sentiment_counts.values())
        plt.xlabel('Sentiment')
        plt.ylabel('Count')
        plt.title('Sentiment Analysis')
        plt.show()

    def aggregate_sentiment_scores(self, data):
        
        # Aggregates sentiment scores.
        
        bullish_scores = [item['sentiment_score'] for item in data if item['sentiment'] == 'Bullish']
        bearish_scores = [item['sentiment_score'] for item in data if item['sentiment'] == 'Bearish']

        bullish_avg = sum(bullish_scores) / len(bullish_scores) if bullish_scores else 0
        bearish_avg = sum(bearish_scores) / len(bearish_scores) if bearish_scores else 0

        return {'Bullish Average': bullish_avg, 'Bearish Average': bearish_avg}

    def search_ticker_sentiment(self, data, ticker):
        
      # Searches for sentiment associated with a specific ticker.
       
        for item in data:
            if item['ticker'] == ticker:
                return item['sentiment']
        return None

if __name__ == "__main__":
    processor = Analysis()
    
    # Fetch data from API
    data_from_api = processor.fetch_data_from_api()
    print("Data fetched from API:")
    print(data_from_api)
    
    # Insert data into Redis
    num_inserted = processor.insert_into_redis(data_from_api)
    print(f"\nInserted {num_inserted} items into Redis.")
    
    # Fetch data from Redis
    data_from_redis = processor.fetch_data_from_redis()
    print("\nData fetched from Redis:")
    print(data_from_redis)
    
    # Processing functions
    processor.generate_sentiment_chart(data_from_redis)
    print("\nAggregate sentiment scores:")
    print(processor.aggregate_sentiment_scores(data_from_redis))
    ticker = 'TSLA'
    print(f"\nSentiment associated with ticker '{ticker}': {processor.search_ticker_sentiment(data_from_redis, ticker)}")
