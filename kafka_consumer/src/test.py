import requests
import logging
import json
from concurrent.futures import ThreadPoolExecutor
import model
import pandas as pd
import time


def get_historical_features(base_url):
    # Your specific JSON request data
    request_data = {
        "features": [
            "cgm_hourly_stats:glucose",
            "transformed_timestamp:dayofweek",
            "transformed_timestamp:hour"
        ],
        "entities": [
            {
                "subject_id": 163669001,
                "event_timestamp": 1391581982,
                "glucose": 100
            }
        ]
    }
    try:
        url = f"{base_url}/get_historical_features"  # Use the correct API endpoint
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, json=request_data, headers=headers)

        # Check for success
        if response.status_code == 200:
            return response.json()
        
        # Handle different errors
        elif response.status_code == 422:
            logging.error(f"Unprocessable Entity: {response.json().get('detail', 'Unknown error')}")
        else:
            logging.error(f"Request failed with status code: {response.status_code}")

        return None

    except requests.exceptions.RequestException as e:
        logging.error(f"Connection error: {e}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON response: {e}")
        return None
    
def get_online_features(base_url):
    request_data = {
        "entities":[{"subject_id": 163669001}],
        "feature_service": "cgm_activity_v3"
    }
    try:
        url = f"{base_url}/get_online_features"  # Use the correct API endpoint
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, json=request_data, headers=headers)

        # Check for success
        if response.status_code == 200:
            return response.json()
        
        # Handle different errors
        elif response.status_code == 422:
            logging.error(f"Unprocessable Entity: {response.json().get('detail', 'Unknown error')}")
        else:
            logging.error(f"Request failed with status code: {response.status_code}")

        return None

    except requests.exceptions.RequestException as e:
        logging.error(f"Connection error: {e}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON response: {e}")
        return None
    
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

"""
features_df = pd.DataFrame([response_data])
print(f"Before prediction features_df is {features_df}")
expected_df = model.expected_glucose(features_df, "../model/glucose_prediction_model-v1.pkl")
print(f"After prediction expected_df is {expected_df}")
"""

def query(store):
    if(store=="online"):
        response_data = get_online_features("http://127.0.0.1:8000")
    else:
        response_data = get_historical_features("http://127.0.0.1:8000")

def measure_performance(store, num_queries=1, num_parallel=1):
    start_time = time.perf_counter_ns()
    # Replace with your actual query
    with ThreadPoolExecutor(max_workers=num_parallel) as executor:
        futures = [executor.submit(query, store) for _ in range(num_queries)]
        for future in futures:
            future.result()  # Wait for each future to complete
    end_time = time.perf_counter_ns()
    total_time = end_time - start_time
    return total_time, total_time / num_queries

print(f"performance online : {measure_performance('online')}")
print(f"performance offline : {measure_performance('offline')}")