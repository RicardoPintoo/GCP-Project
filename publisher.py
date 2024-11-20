from google.cloud import pubsub_v1
import pandas as pd
import json
import time

# Replace with your Google Cloud project and Pub/Sub topic
PROJECT_ID = "moonlit-app-441813-v9"
TOPIC_NAME = "dev-user-events-topic"
CSV_FILE = "input_mockdata.csv"  # Path to your CSV file

def publish_messages(publisher, topic_path, data):
    for i, message in enumerate(data):
        try:
            time.sleep(10)
            future = publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
            print(f"Published message {i} ID: {future.result()}")
        except Exception as e:
            print(f"Error publishing message: {e}")

def main():
    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv(CSV_FILE)

    # Convert the DataFrame rows to a list of dictionaries (records)
    data = df.to_dict(orient="records")

    # Initialize the Pub/Sub publisher
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

    # Publish each row as a message
    print(f"Publishing {len(data)} messages to Pub/Sub...")
    publish_messages(publisher, topic_path, data)

if __name__ == "__main__":
    main()
