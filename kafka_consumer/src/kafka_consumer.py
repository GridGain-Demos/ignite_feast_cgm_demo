import asyncio
import json

import requests
import pandas as pd
from datetime import datetime

from aiokafka import AIOKafkaConsumer # Use AIOKafkaConsumer 
import websockets
import logging
import model as model
import argparse
import time
import sys
import ssl
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Global variable to track WebSocket connections
connected_websockets = set() 

async def consume_and_push_data_and_broadcast(store, base_url, id_field_str, feature_service, env, config, kafka_topic, model_filepath):
    """Consumes messages from Kafka, pushes to Feast, fetches from Feast, and broadcasts to WebSockets.

    This function continuously consumes CGM data from Kafka, pushes it to the 
    Feast online store, and broadcasts the consumed data to connected 
    WebSocket clients. 
    Additionally, it now immediately fetches the record from Feast online store for the latest feature
    and broadcast it to all connected clients via WebSocket.
    
    It handles errors during Kafka consumption and pushing to / fetching from Feast.
    """

    if(env!="local"):
        # Create SSL context (using system truststore)
        ssl_context = ssl.create_default_context()  # Use system's default context
        ssl_context.check_hostname = False          # Disable hostname verification (less secure)
    
        consumer = AIOKafkaConsumer(   # Use AIOKafkaConsumer 
            kafka_topic,
            ssl_context=ssl_context,
            **config,
            group_id=store,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
        )
    else:
        consumer = AIOKafkaConsumer(   # Use AIOKafkaConsumer 
            kafka_topic,
            **config,
            group_id=store,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
        )

    await consumer.start()
    try:
        count = 1
        async for msg in consumer:
            try:
                data = msg.value                
                # Broadcast to connected clients if any
                if connected_websockets:
                    websockets.broadcast(connected_websockets, json.dumps({"type": "consumed", "message": data}))
                
                #logging.debug(f"data is {data}\n\n")

                #Push into the store (online by default)
                push_request = {
                    "push_source_name": "cgm_stats_push_source",
                    "store_type": "online",
                    "event": data  # Nest the existing event data
                }
                
                if(store=="offline"):
                    push_request["store_type"] = "offline"

                push_request = json.dumps(push_request)
                response = push_stream_event(base_url, push_request)

                if(store=="online"):
                    id_value = data[id_field_str]
                    fetch_request = json.dumps({
                        "entities":[{id_field_str: id_value}],
                        "feature_service" : feature_service
                    })

                    #logging.debug(f"fetch_request is {fetch_request}")
                    #get features from online store
                    start_time = time.perf_counter_ns()
                    response = get_online_features(base_url,fetch_request)
                    end_time = time.perf_counter_ns()
                    total_time = end_time - start_time
                else:
                    fetch_request = {
                        "features": [
                            "cgm_hourly_stats:glucose",
                            "transformed_timestamp:dayofweek",
                            "transformed_timestamp:hour"
                        ],
                        "entities": [
                            {
                                "subject_id": 163669001,
                                "event_timestamp": 1391581982,
                            }
                        ]
                    }
                    start_time = time.perf_counter_ns()
                    response = get_historical_features(base_url, fetch_request)
                    end_time = time.perf_counter_ns()
                    total_time = end_time - start_time

                features = response.json()
                if(store=="offline"):
                    print(f"Offline features : {features}")
                    features = convert_format(features)

                if features:
                    features_df = pd.DataFrame([features])
                    expected_df = model.expected_glucose(features_df, model_filepath)
                    logging.info(f'[{count}]: subject [{data["subject_id"]}] timestamp [{data["event_timestamp"]}] actual glucose [{data["glucose"]}] expected glucose [{expected_df["adjusted_glucose"].iloc[0]}] timetaken {total_time}')
    
                count = count + 1
                
            except Exception as e:
                logging.exception("Error receiving message from Kafka")
            finally:
                time.sleep(1)  # Wait for 1 second before next message
    except KeyboardInterrupt:
        print("Consumer shutting down gracefully...")
    except asyncio.CancelledError:  
        pass  # Ignore CancelledError to avoid printing the traceback
    finally:
        await consumer.stop() # Ensure aiokafka consumer is closed properly on shutdown
        print(f"Consumer stopped successfully")

def push_stream_event(base_url, event_data):
    """Synchronously fetches online features from the Feast API."""
    try:
        url = f"{base_url}/push_stream_event"
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, data=event_data, headers=headers)
        return response
    except requests.exceptions.RequestException as e:
        # Log the error appropriately
        logging.exception(f"Error fetching online features")
        return None  # Or handle the error in a way that suits your application
    
def get_historical_features(base_url, fetch_request):
    try:
        url = f"{base_url}/get_historical_features"  # Use the correct API endpoint
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, json=fetch_request, headers=headers)
        return response
    except requests.exceptions.RequestException as e:
        # Log the error appropriately
        logging.exception("Error fetching offline features:")
        return None  # Or handle the error in a way that suits your application

def get_online_features(base_url, fetch_request):
    """Asynchronously pushes an event to the Feast API."""
    try:
        url = f"{base_url}/get_online_features"
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, data=fetch_request, headers=headers)
        return response
    except requests.exceptions.RequestException as e:
        # Log the error appropriately
        logging.error(f"Error pushing event to Feast: {e}")
        return None  # Or handle the error in a way that suits your application

def convert_format(data):
    """Converts data from the list of dictionaries format to the desired format.

    Args:
        data (list): List of dictionaries containing the data to convert.

    Returns:
        dict: The converted data in the desired format.
    """

    # Check if data is empty
    if not data:
        return {}

    # Make a copy of the first element to get keys
    converted_data = {key: [] for key in data[0]}

    for item in data:
        # Ensure values are lists for consistency
        for key, value in item.items():
            converted_data[key].append(value)

    return converted_data

async def main(store, base_url, id_field, feature_service, env, config, kafka_topic, model_filepath):
    """
    Starts the WebSocket server and runs the consumer task concurrently.
    """
    try:
        await consume_and_push_data_and_broadcast(store, base_url, id_field, feature_service, env, config, kafka_topic, model_filepath)
    except KeyboardInterrupt:
        print("Shutting down the server...")

def convert_timestamp(data):
    if "event_timestamp" in data:
        try:
            # Convert event_timestamp from Unix timestamp to datetime object (UTC)
            event_timestamp_dt = datetime.utcfromtimestamp(data["event_timestamp"])

            # Format datetime object to the desired string format
            formatted_timestamp = event_timestamp_dt.strftime("%Y-%m-%d %H:%M:%S.%f")

            # Update the data dictionary with the formatted timestamp
            data["event_timestamp"] = formatted_timestamp
        except (ValueError, OSError):
            # Handle potential errors in timestamp conversion
            raise ValueError("Invalid timestamp format for event_timestamp")

def get_kafka_config(env):
    """Retrieves Kafka configuration based on command line arguments and configuration files."""
    # Default to local configuration
    file_path = "config/"+env+"_config.json" 
    print(f"trying to load {file_path}")
    try:
        with open(file_path, 'r') as config_file:
            config = json.load(config_file)
            return config
    except (FileNotFoundError, KeyError) as e:
        raise Exception(f"Error loading local configuration: {e}")
   
        
def get_env_from_config_path(config_file_path):
    """Extracts the environment name ("aws" or "local") from a configuration file path.

    Args:
        config_path (str): The path to the configuration file (e.g., 'config/aws_config.json').

    Returns:
        str: The environment name ("aws" or "local") or None if not found.
    """

    filename = os.path.basename(config_file_path)  # Get the filename (e.g., 'aws_config.json')
    base_name, _ = os.path.splitext(filename)  # Remove extension (e.g., 'aws_config')

    env_name = None
    if base_name.endswith("_config"):  
        env_name = base_name.removesuffix("_config")  # Directly remove "_config"

    return env_name
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CGM Data Processing with WebSocket")  # Updated description
    parser.add_argument("--store", type=str, default="online", help="Which feature store online or offline")
    parser.add_argument("--base-url", type=str, default="http://127.0.0.1:8000", help="URL of the feature server")
    parser.add_argument("--id-field", type=str, default="subject_id", help="Name of the id field")
    parser.add_argument("--feature-service", type=str, default="cgm_activity_v3", help="Name of the Feature Service")
    parser.add_argument("--env", type=str, default="local", choices=["local", "docker", "aws"], help="Configuration source (local or aws)")
    parser.add_argument("--kafka-topic", type=str, default="cgm_readings", help="Kafka topic")
    parser.add_argument("--model-filepath", type=str, default="model/glucose_prediction_model-v1.pkl", help="Kafka topic")
    args = parser.parse_args()

    args = parser.parse_args()
    print(f"env is {args.env}")
    config = get_kafka_config(args.env)

    print(f"connecting to {config['bootstrap_servers']}")
    # Call the main function with all parsed arguments
    asyncio.run(main(args.store, args.base_url, args.id_field, args.feature_service, args.env, config, args.kafka_topic, args.model_filepath))
