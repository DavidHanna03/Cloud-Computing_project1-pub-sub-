from google.cloud import pubsub_v1
import csv
import json
import os
import glob
# Set Google Cloud credentials
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];
# Define Google Cloud Pub/Sub topic
project_id = "cc-project-milestone-1"
topic_name = "design.dh"  # Updated to match your topic

# Initialize Pub/Sub Publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

# Read CSV and Publish Messages
with open("Labels.csv", "r") as csvfile:
    reader = csv.DictReader(csvfile)  # Read CSV as a dictionary
    for row in reader:
        message = json.dumps(row).encode("utf-8")  # Serialize row
        print(f"Publishing: {row}")
        future = publisher.publish(topic_path, message)
        future.result()  # Ensure message is published

print("All messages published successfully!")

