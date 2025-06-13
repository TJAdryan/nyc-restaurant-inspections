import requests
import pandas as pd
from datetime import date, timedelta
from dotenv import load_dotenv
import os

# --- Configuration ---

# Load environment variables (for your API token) from the .env file
load_dotenv()
# Retrieve the API token. The 'X-App-Token' header requires a non-empty value.
myappsec = os.getenv('MY_APP_SEC')
headers = {'X-App-Token': myappsec} if myappsec else {} # Include token if available

# Define the dataset URL for NYC Restaurant Inspection Data
# This is the Socrata API endpoint for the DOHMH New York City Restaurant Inspection Results.
DATA_SET_URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"

# Date Range Configuration
# We'll pull data for a range ending precisely 30 days ago from today.
end_date = date.today() - timedelta(days=30) # This is the "closest" (most recent) date for the data pull.
start_date = end_date - timedelta(days=90) # Example: pull data for the 90 days before the end date.
                                           # Adjust this timedelta to change the length of your historical data.

# Format dates for the API query.
# The NYC Open Data API for this dataset expects dates in 'YYYY-MM-DD' format for filtering on 'inspection_date'.
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

# API Call Parameters
LIMIT = 1000 # Max number of records to retrieve per API call. Socrata APIs typically have a default limit of 1000.
# Order the results for consistent pagination.
# Ordering by inspection_date (descending) means newest records first, then by CAMIS (a unique restaurant ID).
ORDER_BY = "inspection_date DESC, camis"

print(f"Starting data fetch for NYC Restaurant Inspections from {start_date_str} to {end_date_str}")

# --- Initial API Call ---

# Construct the initial API URL with the date range filter and ordering.
# The '$where' clause is crucial for filtering data based on the 'inspection_date' column.
# 'between' operator is used for specifying a date range.
initial_call_url = (
    f"{DATA_SET_URL}?$limit={LIMIT}"
    f"&$where=inspection_date between '{start_date_str}' and '{end_date_str}'"
    f"&$order={ORDER_BY}"
)

print(f"Initial API Call URL: {initial_call_url}")

# Attempt the first API call to retrieve initial data.
try:
    result = requests.get(initial_call_url, headers=headers)
    result.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx response codes).
    initial_data = result.json() # Parse the JSON response into a Python list of dictionaries.
    df = pd.DataFrame(initial_data) # Convert the initial data into a pandas DataFrame.
    print(f"Successfully fetched {len(initial_data)} records in initial call.")

except requests.exceptions.RequestException as e:
    # Handle any request-related errors (e.g., network issues, invalid URL, HTTP errors).
    print(f"Error during initial API call: {e}")
    initial_data = [] # Ensure initial_data is an empty list if an error occurs, to prevent further errors.
    df = pd.DataFrame() # Ensure df is an empty DataFrame on error.

# --- Paginate and Retrieve All Data ---

offset = 0 # Initialize offset for pagination.
# Continue fetching data as long as the last call returned the maximum number of records (LIMIT).
# This indicates that there might be more data available beyond the current offset.
while len(initial_data) == LIMIT:
    offset += LIMIT # Increment the offset to request the next batch of records.
    print(f"Fetching records with offset: {offset}")

    # Construct the URL for subsequent paginated API calls, including the '$offset' parameter.
    paginated_call_url = (
        f"{DATA_SET_URL}?$limit={LIMIT}"
        f"&$where=inspection_date between '{start_date_str}' and '{end_date_str}'"
        f"&$order={ORDER_BY}"
        f"&$offset={offset}"
    )

    try:
        result = requests.get(paginated_call_url, headers=headers)
        result.raise_for_status()
        paginated_data = result.json()

        if not paginated_data:
            # If an API call returns an empty list, it means there's no more data for the given query.
            print("No more records to fetch.")
            break # Exit the loop.

        # Concatenate the newly fetched data to the existing DataFrame.
        # ignore_index=True prevents pandas from keeping the original index, creating a new continuous index.
        df = pd.concat([df, pd.DataFrame(paginated_data)], ignore_index=True)
        initial_data = paginated_data # Update initial_data to check the loop condition for the next iteration.
        print(f"Fetched {len(paginated_data)} records with offset {offset}. Total records so far: {len(df)}")

    except requests.exceptions.RequestException as e:
        # Handle errors during pagination.
        print(f"Error during paginated API call (offset {offset}): {e}")
        break # Break on error to prevent potential infinite loops or repeated errors.

print(f"\nTotal records retrieved: {len(df)}")

# --- Data Cleaning and Formatting ---

if not df.empty: # Proceed with data cleaning only if the DataFrame is not empty.
    # Convert the 'inspection_date' column to datetime objects for easier manipulation and analysis.
    # 'errors='coerce'' will turn any parsing errors into NaT (Not a Time), preventing the script from crashing.
    df['inspection_date'] = pd.to_datetime(df['inspection_date'], errors='coerce')

    # Define a list of desired columns relevant to restaurant inspection data.
    # This helps in selecting specific fields and ensuring consistency in output.
    desired_columns = [
        'camis', 'dba', 'boro', 'building', 'street', 'zipcode', 'phone',
        'cuisine_description', 'inspection_date', 'action', 'violation_code',
        'violation_description', 'critical_flag', 'score', 'grade',
        'grade_date', 'record_date', 'inspection_type'
    ]

    # Filter the DataFrame to include only the columns that actually exist in the fetched data.
    # This handles cases where some desired columns might not be present in every record.
    existing_columns = [col for col in desired_columns if col in df.columns]
    df = df[existing_columns]

    # Display the first few rows of the processed DataFrame for a quick inspection.
    print("\nFirst 5 rows of the DataFrame:")
    print(df.head())
    # Print a summary of the DataFrame including column data types and non-null values.
    print("\nDataFrame Info:")
    df.info()

    # --- Save Data (Optional) ---
    # Define filenames for the output CSV and Parquet files based on the date range.
    # This helps in organizing and identifying the saved data.
    file_name_csv = f"nyc_restaurant_inspections_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.csv"
    file_name_parquet = f"nyc_restaurant_inspections_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.parquet"

    # Attempt to save the DataFrame to CSV and Parquet formats.
    try:
        df.to_csv(file_name_csv, index=False) # Save to CSV, without writing the DataFrame index.
        print(f"\nData saved to {file_name_csv}")
        df.to_parquet(file_name_parquet, index=False) # Save to Parquet, without writing the DataFrame index.
        print(f"Data saved to {file_name_parquet}")
    except Exception as e:
        # Catch any errors that might occur during file saving.
        print(f"Error saving files: {e}")
else:
    print("No data retrieved to process or save.")

