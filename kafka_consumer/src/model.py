import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import logging
import joblib
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def train_model(data_filepath, model_filepath):
    """
    This function trains a multiple linear regression model to predict glucose levels
    and saves the trained model to a specified file path.

    Parameters:
        data_filepath (str): The path to the parquet file containing the training data.
        model_filepath (str): The path to save the trained model.

    Returns:
        None: The function saves the model to the specified path.
    """
    # Load data from parquet file
    data = pd.read_parquet(data_filepath, engine='pyarrow')  # Specify engine for compatibility

    # Separate features (X) and target variable (y)
    X = data[['subject_id', 'dayofweek', 'hour']]
    y = data['glucose']

    # Split data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Initialize and train the Linear Regression model
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Make predictions on the test set
    y_pred = model.predict(X_test)

    # Evaluate model performance
    mse = mean_squared_error(y_test, y_pred)
    print(f"Mean Squared Error: {mse:.2f}")

    # Save the trained model using joblib
    joblib.dump(model, model_filepath)
    print(f"Model saved to {model_filepath}")

def get_model(model_filepath):
    #TODO : Needs to be parameterized
    try:
        # Load pre-trained model for glucose prediction (same as transformed_timestamp_fresh)
        model = joblib.load(model_filepath)
        return model
    except FileNotFoundError as e:
        logging.error(f"Model file not found: {e}")  # Log the error
        raise  # Re-raise the exception to signal failure

def expected_glucose(inputs: pd.DataFrame, model_filepath) -> pd.DataFrame:
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

    #TODO : Needs to be parameterized
    try:
        model = get_model(model_filepath)
    except FileNotFoundError as e:
        logging.error(f"Model file not found: {e}")  # Log the error
        raise  # Re-raise the exception to signal failure

    #logging.debug(f"Received inputs with columns: {inputs.columns}")  # Helpful for debugging
    #logging.debug(f"Sample data:\n{inputs.head()}")  # Show first few rows

    # Flatten nested columns, assuming they contain dictionaries or lists
    for col in inputs.columns:
        if not pd.api.types.is_numeric_dtype(inputs[col]):
            try:
                inputs[col] = inputs[col].apply(lambda x: x[0] if isinstance(x, (list, dict)) else x)
            except (TypeError, IndexError) as e:
                logging.warning(f"Error flattening column '{col}': {e}")

    # Ensure columns are the correct types for the model
    inputs['subject_id'] = pd.to_numeric(inputs['subject_id'], errors='coerce').astype(pd.Int64Dtype())
    inputs['dayofweek'] = pd.to_numeric(inputs['dayofweek'], errors='coerce')
    inputs['hour'] = pd.to_numeric(inputs['hour'], errors='coerce')

    # Drop rows with any NaN values in the key columns
    inputs = inputs.dropna(subset=["subject_id", "dayofweek", "hour"])

    # Validate required columns are present (you might want to keep this check)
    required_cols = ["subject_id", "dayofweek", "hour"]
    for col in required_cols:
        if col not in inputs.columns:
            raise ValueError(f"Missing required column: {col}")

    # Prepare input features for prediction (unchanged)
    X = inputs[["subject_id", "dayofweek", "hour"]]
    #logging.debug(f"X is \n{X}")

    # Make predictions
    predictions = model.predict(X)
    return pd.DataFrame({"adjusted_glucose": predictions})


# Call the function to train and save the model
if __name__ == "__main__":
    train_model('../data/cgm-stats.parquet', "../model/glucose_prediction_model-v1.pkl")