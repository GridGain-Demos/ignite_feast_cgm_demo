# This is a feature definition file for CGM data

from datetime import timedelta
import pandas as pd
from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    PushSource,
    RequestSource,
)
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Int64, Int32, UnixTimestamp
# TODO Going with default logging on the console; will switch to file based logging through config
import logging

# Subject entity representing the CGM user 
# This entity defines the user of the Continuous Glucose Monitor (CGM) device
subject = Entity(name="subject", join_keys=["subject_id"])

# File data source for CGM hourly statistics
# Loads data from the specified parquet file containing hourly CGM statistics
cgm_stats_source = FileSource(
    name="cgm_hourly_stats_source",
    path="data/cgm_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Feature view for CGM hourly statistics
# Defines how to access and serve pre-computed CGM statistics from the data source
cgm_stats_fv = FeatureView(
    name="cgm_hourly_stats", # Unique identifier for this feature view
    entities=[subject], # Associates the feature view with the 'subject' entity
    ttl=timedelta(days=1), # How long features are valid (1 day in this case)
    
    # Defines the schema for the feature view
    schema=[
        Field(name="subject_id", dtype=Int64),
        Field(name="glucose", dtype=Int64),
        Field(name="event_timestamp", dtype=UnixTimestamp),
    ],
    online=True, # Makes the feature view available for online serving
    source=cgm_stats_source, # Source of the data for this feature view
    tags={"team": "subject"},  # Optional tags for metadata
)

# Request data source for additional input values
# Captures additional input values provided at the time of request (e.g., dayofweek, hour)
input_request = RequestSource(
    name="vals_to_add", # Unique name for the request source
    # Defines the expected input schema
    schema=[
        
    ],
)


# On-demand feature view for estimated glucose
# Generates adjusted glucose predictions based on existing features and input request values
@on_demand_feature_view(
    sources=[cgm_stats_fv, input_request], # Uses historical CGM data and current input
    # Defines the output schema of this feature view
    schema=[
        Field(name="dayofweek", dtype=Int32),
        Field(name="hour", dtype=Int32),
    ],
)

def transformed_timestamp(inputs: pd.DataFrame) -> pd.DataFrame:
    """Predicts adjusted glucose levels using a pre-trained model.
    
    Args:
        inputs: DataFrame containing subject_id, dayofweek, and hour.

    Returns:
        DataFrame with a single column 'adjusted_glucose' containing the predictions.

    Note:
        Handles missing subject_id with a temporary placeholder value. This should be 
        replaced with proper handling for new subjects or requests without IDs.
    """
    #timestamp = pd.to_datetime(inputs["event_timestamp"])[0]
    ##print(f"transformed_timestamp_fresh : {timestamp}")
    #dayofweek = timestamp.dayofweek
    #hour = timestamp.hour
    #print("----inputs is ----")
    #print(inputs)
    #inputs['event_timestamp'] = pd.to_datetime("2024-06-16 21:55:32.477849+00:00")
    inputs["event_timestamp"] = pd.to_datetime(inputs["event_timestamp"])
    df = pd.DataFrame()
    df["dayofweek"] = inputs["event_timestamp"].dt.dayofweek
    df["hour"] = inputs["event_timestamp"].dt.hour
    #print("----sent df is ----")
    #print(df)
    return df
    


# Feature service for CGM activity model
# Bundles the raw CGM stats and the estimated glucose feature view for model
cgm_activity_v1 = FeatureService( 
    name="cgm_activity_v1", # Unique identifier for the feature service
    # Features included in the feature service
    features=[
        cgm_stats_fv,
        transformed_timestamp,  # Selects all features from the feature view
    ],
)

# Push data source for CGM statistics
# Enables pushing CGM statistics directly to Feast for real-time updates
cgm_stats_push_source = PushSource(
    name="cgm_stats_push_source",
    batch_source=cgm_stats_source,
)

# Feature view for fresh CGM hourly statistics
# Same as cgm_stats_fv but uses the push source for real-time data updates
cgm_stats_fresh_fv = FeatureView(
    name="cgm_hourly_stats_fresh",
    entities=[subject],
    ttl=timedelta(days=1),
    schema=[
        Field(name="subject_id", dtype=Int64),
        Field(name="glucose", dtype=Int64),
        Field(name="event_timestamp", dtype=UnixTimestamp),
    ],
    online=True,
    source=cgm_stats_push_source,  # Uses the push source for fresh data
    tags={"team": "cgm_performance"}, # Different team tag for the fresh feature view
)


# On-demand feature view for estimated glucose (fresh)
# Generates adjusted glucose predictions using fresh CGM data
@on_demand_feature_view(
    sources=[cgm_stats_fresh_fv, input_request],  # relies on fresh version of FV
    schema=[ 
        Field(name="dayofweek", dtype=Int32),
        Field(name="hour", dtype=Int32),
    ],
)

def transformed_timestamp_fresh(inputs: pd.DataFrame) -> pd.DataFrame:
    """
    Predicts adjusted glucose levels using a pre-trained model and fresh CGM data.
    
    Args:
        inputs: DataFrame containing subject_id, dayofweek, and hour.

    Returns:
        DataFrame with a single column 'adjusted_glucose' containing the predictions.

    Note:
        Handles missing subject_id with a temporary placeholder value. This should be
        replaced with proper handling for new subjects or requests without IDs.
    """
    df = pd.DataFrame()
    #timestamp = pd.to_datetime(inputs["event_timestamp"])[0]
    #print(f"transformed_timestamp_fresh : {timestamp}")
    #dayofweek = timestamp.dayofweek
    #hour = timestamp.hour
    #print("----inputs is ----")
    #print(inputs)
    #inputs["glucose"] = 100
    #inputs['event_timestamp'] = pd.to_datetime("2024-06-16 21:55:32.477849+00:00")
    inputs["event_timestamp"] = pd.to_datetime(inputs["event_timestamp"])
    df = pd.DataFrame()
    df["dayofweek"] = inputs["event_timestamp"].dt.dayofweek
    df["hour"] = inputs["event_timestamp"].dt.hour
    #print("----sent df is ----")
    #print(df)
    return df
    

# Feature service for CGM activity model v3
# Combines fresh CGM stats and the fresh estimated glucose feature for version 3
cgm_activity_v3 = FeatureService(
    name="cgm_activity_v3", # Uses the fresh feature view
    features=[cgm_stats_fresh_fv, transformed_timestamp_fresh], # Uses the fresh on-demand feature view
)