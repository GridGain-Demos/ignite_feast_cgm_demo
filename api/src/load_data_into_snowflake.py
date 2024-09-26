import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def load_data_into_snowflake(parquet_file_path, snowflake_credentials):
    try:
        # Read Parquet file into Pandas DataFrame
        df = pd.read_parquet(parquet_file_path, engine='pyarrow')
        print("DataFrame dtypes before conversion:")
        print(df.dtypes)

        # Convert timestamp columns to Snowflake format
        for col in ["event_timestamp", "created"]:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d %H:%M:%S.%f").dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        print("\nDataFrame dtypes after conversion:")
        print(df.dtypes)

        # Connect to Snowflake
        conn = snowflake.connector.connect(**snowflake_credentials)
        
        # Create cursor
        cur = conn.cursor()
        
        # Fully qualify the table name (using double quotes for case sensitivity)
        full_table_name = f'"{snowflake_credentials["database"]}"."{snowflake_credentials["schema"]}".cgm_hourly_stats'
        
        """
        # Create table (if it doesn't exist)
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            event_timestamp TIMESTAMP,
            subject_id INT,
            glucose INT,
            created TIMESTAMP
        )
        
        print(f"\nExecuting CREATE TABLE statement: {create_table_sql}")
        cur.execute(create_table_sql)
        conn.commit()
        print("CREATE TABLE statement executed and committed successfully")
        """
        
        # Verify table exists
        cur.execute(f'SHOW TABLES LIKE \'cgm_hourly_stats\' IN SCHEMA "{snowflake_credentials["database"]}"."{snowflake_credentials["schema"]}"')
        if not cur.fetchone():
            raise Exception(f"Table {full_table_name} was not created successfully")
        else:
            print(f"Table {full_table_name} exists")
        
        # Upload data to Snowflake
        print(f"\nUploading data to table: {full_table_name}")
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name='cgm_hourly_stats',
            schema=snowflake_credentials["schema"],
            database=snowflake_credentials["database"]
        )
        
        if success:
            print(f"Successfully loaded {nrows} rows into Snowflake.")
        else:
            print("Data load failed.")
    
    except Exception as e:
        print(f"Error loading data into Snowflake: {e}")
    
    finally:
        # Close connection
        if 'conn' in locals():
            conn.close()

# Snowflake credentials
snowflake_credentials = {
    "user": "maninizetta",
    "password": "L8tj5yQgEWBSpb5",
    "account": "evsqlzd-nub69597",
    "warehouse": "FEASTTEST",
    "database": "FEASTTEST",
    "schema": "PUBLIC"
}

# Call the function to load data
load_data_into_snowflake("data/cgm_stats.parquet", snowflake_credentials)