import requests
import pandas as pd
from pymongo import MongoClient

def fetch_data(api_url, params):
    """ Fetch data from API and save it as CSV. """
    response = requests.get(api_url, params=params)
    data = response.json()  # Assuming the response is JSON
    df = pd.DataFrame(data)
    df.to_csv('output.csv', index=False)
    return df

def process_data(input_csv):
    """ Load and process data from CSV. """
    df = pd.read_csv(input_csv)
    # Example processing: create a new column by doubling the values of an existing column
    df['processed_value'] = df['existing_column'].apply(lambda x: x * 2)
    return df

def save_to_mongo(df, db_name, collection_name):
    """ Save DataFrame to MongoDB collection. """
    client = MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    collection = db[collection_name]
    records = df.to_dict('records')
    collection.insert_many(records)

def run_pipeline(api_url, params, input_csv, db_name, collection_name):
    """ Run the data pipeline from API fetch to MongoDB storage. """
    print("Fetching data...")
    df_fetched = fetch_data(api_url, params)
    print("Data fetched successfully.")

    print("Processing data...")
    df_processed = process_data(input_csv)
    print("Data processed successfully.")

    print("Saving data to MongoDB...")
    save_to_mongo(df_processed, db_name, collection_name)
    print("Data saved to MongoDB successfully.")

if __name__ == "__main__":
    api_url = 'https://api.example.com/data'
    params = {'param1': 'value1', 'param2': 'value2'}
    input_csv = 'output.csv'
    db_name = 'yourDatabase'
    collection_name = 'yourCollection'

    run_pipeline(api_url, params, input_csv, db_name, collection_name)
