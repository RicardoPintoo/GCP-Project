import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode
from google.cloud import firestore
from dotenv import load_dotenv
import logging
import json
import os

# Load environment variables from .env file
load_dotenv()

# Access environment variables
project_id = os.getenv('PROJECT_ID')
region = os.getenv('REGION')
topic_name = os.getenv('TOPIC_NAME')
dataset_name = os.getenv('DATASET_NAME')
table_name = os.getenv('TABLE_NAME')

# Define the folder and log file name
log_folder = "logs"
log_file = os.path.join(log_folder, "pubsub_debug_directrunner.log")

# Create the folder if it doesn't exist
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Set up logging
#log_file = "logs/pubsub_debug_directrunner.log"
logging.basicConfig(
    filename=log_file,
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Define BigQuery schema
bq_schema = "user_id:INTEGER, event_type:STRING, date:DATE, product_id:INTEGER, page_url:STRING, device_type:STRING, location:STRING, duration_seconds:INTEGER, comment_text:STRING, subscription_plan:STRING, form_id:INTEGER, login_method:STRING, referral_source:STRING, streaming_quality:STRING, cart_items_count:INTEGER, error_message:STRING"

options = PipelineOptions()

# Set Google Cloud project
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project_id
google_cloud_options.region = region
google_cloud_options.temp_location = "gs://dataflow_dataflow/temp"
google_cloud_options.staging_location = "gs://dataflow_dataflow/staging"

# Set pipeline options
standard_options = options.view_as(StandardOptions)
standard_options.view_as(StandardOptions).runner = "DataflowRunner"  # Use DataflowRunner
standard_options.view_as(StandardOptions).streaming = True  # Enable streaming mode

# Function to process the incoming message (modify as needed)
def process_message(message):
    # Import datetime inside the function
    from datetime import datetime
    # Convert to datetime object
    date_obj = datetime.strptime(message["timestamp"], "%m/%d/%Y")
    # Format to YYYY-MM-DD
    formatted_date = date_obj.strftime("%Y-%m-%d")
    # Transforming the message into a dictionary with the schema fields  
    return {
        "user_id": int(message["user_id"]),
        "event_type": message["event_type"],
        "date":  formatted_date,
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
    message_str = message.decode("utf-8")  # Decode bytes to string
    logging.info(f"Message data: {message_str}")
    return message


# Dataflow pipeline
with beam.Pipeline(options=options) as p:
    (
        p
        | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=f"projects/{project_id}/topics/{topic_name}")
        | "Apply Fixed Window" >> beam.WindowInto(FixedWindows(300),
                                                  trigger=AfterProcessingTime(0), # Trigger the window immediately
                                                  accumulation_mode=AccumulationMode.DISCARDING) # Discard all the data than comes after fixedwindows time
        | "Decode Messages" >> beam.Map(lambda msg: msg.decode("utf-8"))  # Decode bytes to string
        | "Parse JSON" >> beam.Map(json.loads)
        | "Process Messages" >> beam.Map(process_message)
        | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            f"{project_id}:{dataset_name}.{table_name}",
            schema=bq_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )
    )
