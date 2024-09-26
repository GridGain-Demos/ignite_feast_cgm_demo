import subprocess
from datetime import datetime
import pandas as pd
from feast import FeatureStore
from feast.data_source import PushMode
import logging

id1 = 163669001
id2 = 163669053
id3 = 2133011
date1 = datetime(2014, 2, 5, 10, 59, 42)
date2 = datetime(2014, 2, 5, 10, 59, 42)
date3 = datetime(2014, 2, 5, 10, 59, 42)

def run_demo():
    """Executes a demonstration of the Feast feature store workflow."""
    store = FeatureStore(repo_path=".")

    try:
        # Step 1: Apply Feature Definitions
        print("--- Run feast apply ---")
        subprocess.run(["feast", "apply"])

        # Step 2: Fetch Historical Features for Training
        print("\n--- Historical features for training ---")
        fetch_historical_features(store)

        # Step 3: Materialize Features to Online Store
        print("\n--- Load features into online store ---")
        store.materialize_incremental(end_date=datetime.now())

        # Step 4: Fetch Online Features (Standard)
        print("\n--- Online features ---")
        fetch_online_features(store)

        # Step 5: Fetch Online Features (Using Feature Service)
        print("\n--- Online features retrieved (instead) through a feature service---")
        fetch_online_features(store, source="feature_service")

        # Step 6: Fetch Online Features (Using Feature View with Push Source)
        print("\n--- Online features retrieved (using feature service v3, which uses a feature view with a push source---")
        fetch_online_features(store, source="push")

        # Step 7: Simulate Stream Event Ingestion
        print("\n--- Simulate a stream event ingestion of the hourly stats df ---")
        event_df = pd.DataFrame.from_dict(
            {
                "subject_id": [id1],
                "event_timestamp": [datetime.now()],
                "created": [datetime.now()],
                "glucose": [120],
            }
        )
        print(f"Pushing event data: {event_df}")
        store.push("cgm_stats_push_source", event_df, to=PushMode.ONLINE_AND_OFFLINE)

        # Step 8: Fetch Updated Online Features
        print("\n--- Online features again with updated values from a stream push---")
        fetch_online_features(store, source="push")
        
    except Exception as e:  # Catching a general exception here for flexibility
        print(f"Unexpected error in demo: {e}")

    finally:
        # Step 11: Teardown (Optional)
        print("\n--- Run feast teardown ---")
        subprocess.run(["feast", "teardown"])

def fetch_historical_features(store: FeatureStore):
    """
    Fetches historical features from the feature store based on entity data.

    Args:
        store (FeatureStore): The Feast feature store.
        for_batch_scoring (bool): Indicates whether the features are being fetched for batch scoring. 
                                If True, the 'event_timestamp' is set to the current time.
    """
    
    # Create a DataFrame of entity data with their join keys, timestamps, and
    # other values used for on-demand transformations.
    entity_df = pd.DataFrame.from_dict(
        {
            "subject_id": [id1, id2, id3],
            "event_timestamp": [
                date1,
                date2,
                date3,
            ],
            "glucose": [100, 120, 140],
            # values we're using for an on-demand transformation
        }
    )

    #Uncomment for debugging on the console
    #print(f"fetch_historical_features_entity_df : entity_df IS \n{entity_df}")
        
    try:
        # Get historical features from the feature store
        training_df = store.get_historical_features(
            entity_df=entity_df,
            features=[
                "cgm_hourly_stats:glucose",
                "transformed_timestamp:dayofweek",
                "transformed_timestamp:hour",
            ],
        ).to_df()

        print("get_historical_features done")
        print(training_df.head())
    except Exception as e:  # Catching a general exception here for flexibility
        logging.exception("Error fetching historical features:")
        raise


def fetch_online_features(store: FeatureStore, source: str = ""):
    """
    Fetches online features from the feature store.

    Args:
        store (FeatureStore): The Feast feature store.
        source (str): Determines which source to use for fetching features:
            - "": Fetches only the 'adjusted_glucose' feature from the on-demand feature view.
            - "feature_service": Fetches all features from the 'cgm_activity_v1' feature service.
            - "push": Fetches all features from the 'cgm_activity_v3' feature service (which uses push source).
    """
    
    # Define entity data
    entity_rows = [
        {
            "subject_id": id1,
        }
    ]
    try:
        # Determine the features to fetch based on the source
        if source == "feature_service":
            print("source == feature_service")
            features_to_fetch = store.get_feature_service("cgm_activity_v1")
        elif source == "push":
            print("source == push")
            features_to_fetch = store.get_feature_service("cgm_activity_v3")
        else:
            print("source == else")
            features_to_fetch = [
                "cgm_hourly_stats:glucose",
                "transformed_timestamp_fresh:dayofweek",
                "transformed_timestamp_fresh:hour",
            ]

        #Uncomment for debugging on the console
        #print(f"fetch_online_features : entity_rows ARE \n{entity_rows}")
        #print(f"fetch_online_features : features_to_fetch ARE \n{features_to_fetch}")
    
        # Get online features from the feature store
        returned_features = store.get_online_features(
            features=features_to_fetch,
            entity_rows=entity_rows,
        ).to_dict()

        # Print the returned features in a sorted manner
        for key, value in sorted(returned_features.items()):
            print(key, " : ", value)

    except Exception as e:  # Catching a general exception here for flexibility
        logging.exception("Error fetching online features:")
        raise


if __name__ == "__main__":
    run_demo()