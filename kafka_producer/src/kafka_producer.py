import json
import pandas as pd
from kafka import KafkaProducer
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from datetime import datetime
import logging
import argparse
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def produce_cgm_data_and_broadcast(parquet_file_path, env, config, kafka_topic):
    """
    Reads CGM data from a Parquet file, modifies timestamps, 
    sends messages to Kafka, and broadcasts events to WebSocket clients.
    """

    bootstrap_servers = config['bootstrap_servers']
    logging.info(f"Starting to produce CGM data...to {bootstrap_servers}") 
    
    try:
        if(env!="aws"):
            create_kafka_topic(kafka_topic,config)

        # Kafka Producer
        producer = KafkaProducer(
            **config,
            value_serializer=lambda m: json.dumps(m).encode("utf-8")
        )
        logging.info("KafkaProducer created successfully")
        
        df = pd.read_parquet(parquet_file_path)
        count = 1
        for _, row in df.iterrows():
            try:
                #print(f"row is \n{row} \n")
                timestamp = int(datetime.now().timestamp())
                #print(f"timestamp is {timestamp}")
                # Convert the Series to a dictionary before JSON serialization
                row["event_timestamp"] = timestamp
                row["created"] = timestamp
                event_data_dict = row.to_dict()
                #print(f"event_data_dict is {event_data_dict}")
                
                producer.send(kafka_topic, value=event_data_dict)  # Send to Kafka
                logging.info(f"{count}: {event_data_dict}")

                count = count + 1
            except Exception as e:
                logging.exception("Error fetching features")
            finally:
                producer.flush()  # Ensure all messages are sent
                time.sleep(1)
    except FileNotFoundError as e:
        logging.error(f"Parquet file not found: {e}")
    except KeyboardInterrupt:
        print("Producer shutting down gracefully...")
        producer.close()
    except Exception as e:
        logging.exception("Error sending message to Kafka")
    finally:
        print("Producer shutting down finally...")
        if 'producer' in locals():
            producer.close()
        print("Producer shutting down")

def create_kafka_topic(topic_name, config, num_partitions=1, replication_factor=1):
    """Creates a Kafka topic if it doesn't already exist."""

    admin_client = KafkaAdminClient(**config)

    topic_list = []
    topic_list.append(
        NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
    )
    try:
        # Check if topic already exists
        existing_topics = admin_client.list_topics()
        if topic_name in existing_topics:
            logging.warning(f"Topic '{topic_name}' already exists. Skipping creation.")
        else:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logging.info(f"Topic '{topic_name}' created successfully.")

    except TopicAlreadyExistsError:
        logging.warning(f"Topic '{topic_name}' already exists. Skipping creation.")
    except Exception as e:
        logging.error(f"Error creating topic '{topic_name}': {e}")
    finally:
        admin_client.close()
    

def main(parquet_file_path, env, config, kafka_topic):
    """
    Starts the WebSocket server and runs the producer task concurrently.
    """
    # Start WebSocket Server
    try:
        produce_cgm_data_and_broadcast(parquet_file_path, env, config, kafka_topic)
    except Exception as e:
        print(f"Error in main function: {e}")

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
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CGM Data Processing with WebSocket")  # Updated description
    parser.add_argument("--parquet-file-path", type=str, default="data/cgm_stats.parquet", help="Path to the Parquet file")
    parser.add_argument("--env", type=str, default="local", choices=["local", "docker", "aws"], help="Configuration source (local or aws)")
    parser.add_argument("--kafka-topic", type=str, default="cgm_readings", help="Kafka topic")

    args = parser.parse_args()
    print(f"env is {args.env}")
    config = get_kafka_config(args.env)

    print(f"connecting to {config['bootstrap_servers']}")
    # Call the main function with all parsed arguments
    main(args.parquet_file_path, args.env, config, args.kafka_topic)

