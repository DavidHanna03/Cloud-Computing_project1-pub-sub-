from google.cloud import pubsub_v1
import json
import os
import glob

# Set Google Cloud credentials
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];
# Define Google Cloud Pub/Sub subscription
project_id = "cc-project-milestone-1"
subscription_name = "design.dh-sub"  # Updated to match your subscription

# Initialize Pub/Sub Subscriber
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

print(f"Listening for messages on {subscription_path}...\n")

# Define callback function for processing messages
def callback(message):
    data = json.loads(message.data.decode("utf-8"))  # Deserialize message
    print(f"Received record: {data}")
    message.ack()  # Acknowledge message

# Start receiving messages
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("Consumer stopped.")

