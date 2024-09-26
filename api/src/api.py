from fastapi import FastAPI, HTTPException, status, Body
from datetime import datetime
import pandas as pd
from feast import FeatureStore
from feast.data_source import PushMode
from typing import List, Dict, Any
import subprocess
import logging
import sys
import time
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

app = FastAPI()
store = FeatureStore(repo_path=os.environ.get('repo_path', 'repo/local'))

@app.get("/")
async def health_check():
    return {"status": "ok"}

@app.post("/setup")
def setup():
    """
    Triggers the execution of `feast apply` to apply feature definitions within the configured Feast repository.
    """
    # Input validation (same as before)
    try:
        #TODO Need to provide data_path as a parameter to subprocess
        subprocess.run(["feast", "apply"],cwd=os.environ.get('repo_path', 'repo/local'))
        return {"message": "Feast apply completed"}
    except subprocess.CalledProcessError as e:
        logging.exception(f"Error applying features")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error applying features: {e}")

@app.post("/get_historical_features")
def get_historical_features(request_data: Dict = Body(
        ...,
        example={
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
        },
    )):

    """
    This API fetches historical features from the offline feature store, based on provided entity data.

    Body:
        -   features (array of strings): List of feature names to fetch.
        -   entities (array of objects): Entity data. Each object represents an entity and should contain the following:
            -   subject_id (integer or string): Unique identifier for the entity.
            -   event_timestamp (integer): Timestamp representing the event time for the entity data.
            -   Additional fields specific to the entity (e.g., "glucose" in the example).

    """
    try:
        start_time = time.perf_counter_ns()
        #logging.debug(f"request_data IS \n{request_data}")
        entity_data = request_data["entities"]  # List[Dict[str, Any]] Entity data for which the features have to be fetched
        features = request_data["features"]  # List[str] Features that need to be fetched

        convert_timestamps(entity_data)

        #logging.debug(f"entity_data IS \n{entity_data}")
        #logging.debug(f"features IS \n{features}")
        entity_df = pd.DataFrame(entity_data)
        #logging.debug(f"entity_df IS \n{entity_df}")

        log("get_historical_features", "api", "preprocess", time.perf_counter_ns()-start_time)

        start_time_get_historical_features = time.perf_counter_ns()
        training_df = store.get_historical_features(
            entity_df=entity_df,
            features=features,
        ).to_df()

        log("get_historical_features", "api", "store.get_historical_features", time.perf_counter_ns()-start_time_get_historical_features)
        log("get_historical_features", "api", "total", time.perf_counter_ns()-start_time)
        return training_df.to_dict(orient="records")

    except Exception as e:  # Catching a general exception here for flexibility
        logging.exception("Error fetching historical features")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error fetching historical features: {e}")

@app.post("/materialize")
def materialize_features():
    """
    This API materializes features from your offline store to the online store, making them available for real-time serving.
    """
    try:
        store.materialize_incremental(end_date=datetime.now())
        return {"message": "Features materialized to online store"}
    except Exception as e:
        logging.exception("Error materializing features")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error materializing features: {e}")
    
@app.post("/push_stream_event")
def push_stream_event(
    data: Dict = Body(
        ...,
        example={
            "push_source_name": "cgm_stats_push_source",
            "store_type":"online",
            "event":{
                "subject_id": 163669001,
                "event_timestamp": 1718678283,
                "glucose": 135,
                "created": 1718678283
            }
        },
    )
):
    """
    This API simulates a stream event by pushing data into the Feast feature store.

    Body:
        -   push_source_name (string): The name of the push source defined in the Feast repository.
        -   store_type (string): Determines whether data will be pushed to the "online" or "offline" store.
        -   event (object): Data representing the stream event. This should match the schema defined in the push source.
                -   The example includes fields like `subject_id`, `event_timestamp`, `glucose`, and `created`, but can be adjusted these based on the specific feature definitions.

    """
    try:
        feature ="push_stream_event"
        start_time = time.perf_counter_ns()
        push_source_name = data['push_source_name']
        event_data = data['event']
        store_type = data['store_type']
        #Check for timestamps and convert them
        convert_timestamps_in_dict(event_data)
        #logging.debug(f"event_data is {event_data}")
        event_df = pd.DataFrame([event_data])
        #logging.debug(f"event_df is {event_df}")
        pushmode = PushMode.ONLINE
        if(store_type=="offline"):
            pushmode = PushMode.OFFLINE
            feature += "_offline"
        else:
            feature += "_online"
        
        log(feature, "api", "preprocess", time.perf_counter_ns()-start_time)

        start_time_push = time.perf_counter_ns()
        store.push(push_source_name, event_df, to=pushmode)
        log(feature, "api", "store.push", time.perf_counter_ns()-start_time_push)

        log(feature, "api", "total", time.perf_counter_ns()-start_time)
        return {"message": "Stream event simulated successfully"}
    except Exception as e:
        logging.exception("Error simulating stream event")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error simulating stream event: {e}")


@app.post("/get_online_features")
def get_online_features(request_data: Dict = Body(
        ...,
        example={
            "entities":[{"subject_id": 163669001}],
            "feature_service": "cgm_activity_v3"
        },
    )
):
    """
    This API fetches online features from a specific feature service in the Feast feature store.

    Body:
        -   entities (array of objects): Entity data. Each object represents an entity and must contain the following:
            -   The entity's primary key(s) (e.g., "subject_id" in the example)
        -   feature_service (string): The name of the feature service from which to fetch the features.
    """
    try:
        start_time = time.perf_counter_ns()
        entity_data = request_data["entities"]  # Extract entity data as a list of dictionaries
        feature_service_name = request_data["feature_service"]

        #logging.debug(f"entity_data IS \n{entity_data}")
        #logging.debug(f"feature_service_name IS \n{feature_service_name}")

        # Check if the feature service name is provided
        if feature_service_name is None:
            raise HTTPException(
                status_code=400, 
                detail="Missing feature_service name in the request."
            )

        log("get_online_features", "api", "preprocess", time.perf_counter_ns()-start_time)

        start_time_get_feature_service = time.perf_counter_ns()
        try:
            features_to_fetch = store.get_feature_service(feature_service_name)
        except ValueError:
            raise HTTPException(
                status_code=400, 
                detail="Invalid feature_service name."
            )
        log("get_online_features", "api", "store.get_feature_service", time.perf_counter_ns()-start_time_get_feature_service)

        start_time_get_online_features = time.perf_counter_ns()
        returned_features = store.get_online_features(
            features=features_to_fetch, 
            entity_rows=entity_data, 
        ).to_dict()
        log("get_online_features", "api", "store.get_online_features", time.perf_counter_ns()-start_time_get_online_features)

        log("get_online_features", "api", "total", time.perf_counter_ns()-start_time)
        return returned_features

    except Exception as e:
        logging.exception("Error fetching online features")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error fetching online features: {e}")

@app.post("/teardown") 
def teardown_features():
    """
    This API is intended for testing purposes destroys online store, offline database, and registry.
    """
    try:
        subprocess.run(["feast", "teardown"])
        return {"message": "Feast teardown completed"}
    except subprocess.CalledProcessError as e:
        logging.exception("Error during Feast teardown")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error during Feast teardown: {e}")
    

def convert_timestamps(entity_data):
    # Check if it's the list of dictionaries
    for entity_data_item in entity_data: # Iterate through the dictionaries in the list
            convert_timestamps_in_dict(entity_data_item)

def convert_timestamps_in_dict(entity_data_item):
    if "event_timestamp" in entity_data_item:
        try:
            #logging.debug(f"entity_data_item : {entity_data_item}")
            timeInMillis = entity_data_item["event_timestamp"]
            #logging.debug(f"timeInMillis : {timeInMillis}")
            dt_object = datetime.fromtimestamp(timeInMillis)
            #logging.debug(f"dt_object : {dt_object}")
            # Update the entity_data dictionary
            entity_data_item["event_timestamp"] = dt_object
        except ValueError:
            raise ValueError("Invalid timestamp format for event_timestamp")
    else:
        logging.error("convert_timestamps_in_dict : data_list is not a dict")

def log(feature, layer, event, timetaken):
    """
    Function to be called from your other scripts (producer, consumer, online store).
    Prepares the log message and sends it to the central logging server.
    """
    timetakeninmicro = timetaken/1000
    timetakeninmillis = timetakeninmicro/1000
    message = {
        "feature": feature,
        "layer": layer,
        "event": event,
        "timetaken": timetaken,
        "timetakeninmicro": timetakeninmicro,
        "timetakeninmillis": timetakeninmillis
    }
    logging.info(message)  # Run the async function in an event loop
