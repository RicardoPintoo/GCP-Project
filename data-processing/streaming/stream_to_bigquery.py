import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode
import os
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

# Access environment variables
project_id = os.getenv('PROJECT_ID')
region = os.getenv('REGION')
topic_name = os.getenv('TOPIC_NAME')
dataset_name = os.getenv('DATASET_NAME')
table_name = os.getenv('TABLE_NAME')

# Define BigQuery schema
bq_schema = "user_id:INTEGER, event_type:STRING, timestamp:TIMESTAMP, product_id:INTEGER, page_url:STRING, device_type:STRING, location:STRING, duration_seconds:INTEGER, comment_text:STRING, subscription_plan:STRING, form_id:INTEGER, login_method:STRING, referral_source:STRING, streaming_quality:STRING, cart_items_count:INTEGER, error_message:STRING"

options = PipelineOptions()

# Set Google Cloud project
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project_id
google_cloud_options.region = region
google_cloud_options.temp_location = "gs://dataflow_dataflow/temp"
google_cloud_options.staging_location = "gs://dataflow_dataflow/staging"

# Set pipeline options
standard_options = options.view_as(StandardOptions)
standard_options.view_as(StandardOptions).runner = "DirectRunner"  # Use DataflowRunner
standard_options.view_as(StandardOptions).streaming = True  # Enable streaming mode

# Example function to process the incoming message (modify as needed)
def process_message(message):
    # Example of transforming the message into a dictionary with the schema fields
    return {
        "user_id": int(message["user_id"]),
        "event_type": message["event_type"],
        "timestamp": message["timestamp"],
        "product_id": int(message["product_id"]),
        "page_url": message.get("page_url", None),
        "device_type": message.get("device_type", None),
        "location": message.get("location", None),
        "duration_seconds": int(message.get("duration_seconds", 0)),
        "comment_text": message.get("comment_text", None),
        "subscription_plan": message.get("subscription_plan", None),
        "form_id": int(message.get("form_id", 0)),
        "login_method": message.get("login_method", None),
        "referral_source": message.get("referral_source", None),
        "streaming_quality": message.get("streaming_quality", None),
        "cart_items_count": int(message.get("cart_items_count", 0)),
        "error_message": message.get("error_message", None)
    }

# Function to log Pub/Sub messages
def debug_message(message):
    logging.info(f"Message data: {message.data}")
    if hasattr(message, 'attributes'):
        logging.info(f"Attributes: {message.attributes}")
    return message

def parse_pubsub_message(pubsub_message):
    message_data = pubsub_message.data.decode("utf-8")  # Decode bytes to string
    print(message_data)

# Define your Dataflow pipeline
with beam.Pipeline(options=options) as p:
    (
        p
        | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=f"projects/{project_id}/topics/{topic_name}")
        | "Apply Fixed Window" >> beam.WindowInto(FixedWindows(60))
        #| "Write to File" >> beam.io.WriteToText("debug_output.txt")
        #| "Debug Messages" >> beam.Map(debug_message)  # Log each message
        | "Process Messages" >> beam.Map(parse_pubsub_message )
        #| "Write to BigQuery" >> beam.io.WriteToBigQuery(
            #f"{project_id}:{dataset_name}.{table_name}",
            #schema=bq_schema,  # Pass the schema here
            #write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        #)
    )
