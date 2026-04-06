# Imports

import requests
import os
import pandas as pd
import json
import re
import io
import logging
from concurrent.futures import ThreadPoolExecutor

# Constants
API_METASTORE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
LOGS = "metastore_api_logs.txt"
PREVIOUS_RUN_DATA_FILE = "previous_run_data.json"
THEME = "Hospitals"
OUTPUT_DIR_NAME = "output_data"

# Setting up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(LOGS), 
                              logging.StreamHandler()])

logger = logging.getLogger(__name__)
logger.info("Fetching dataset metadata from CMS API...")


#Defining a function to fetch data from the CMS API
def fetch_data_from_metastore():
    try:
        response = requests.get(API_METASTORE_URL)
        response.raise_for_status()
        raw_data = response.json()
        return raw_data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from CMS API: {e}")
        return None

def load_previous_run_data():
    
    # Load previous run data if it exists
    if os.path.exists(PREVIOUS_RUN_DATA_FILE):
        with open(PREVIOUS_RUN_DATA_FILE, "r") as file:
            logger.info("Loading previous run data...")
            return json.load(file)
    logger.info("No previous run data found.")
    return {}

def filter_datasets(datasets, theme):
    
    # Filter datasets based on the specified theme
    filtered = [dataset for dataset in datasets if theme in dataset.get("theme", [])]
    logger.info(f"Filtered datasets for theme '{theme}': {len(filtered)} found.")
    return filtered

def found_new_datasets(previous_data, new_dataset):
    
    # Determine if the new dataset is new or updated compared to previous data
    if previous_data is None:
        return True
    dataset_id = new_dataset["identifier"]
    modified_date = new_dataset.get("modified")
    last = previous_data.get(dataset_id)

    if last is None:
        logger.info(f"New dataset found: {dataset_id}, download needed.")
        return True
    
    if modified_date > last:
        logger.info(f"Dataset updated: {dataset_id}, download needed.")
        return True
    
    logger.info(f"No update for dataset: {dataset_id}, no download needed.")
    return False

def normalize_column_names_to_snake_case(col):

    # Normalize column names to snake_case
    col = re.sub(r'[^\w\s]', '', col)  # Remove special characters
    col = re.sub(r'\_+', '_', col) # Replace multiple underscores with a single one
    col = re.sub(r'\s+', '_', col)  # Replace spaces with underscores
    col = re.sub(r'[^\w]+', '_', col) # Replace any remaining non-alphanumeric characters with underscores
    col = col.lower()

    return col

def transform_data(dataset):
    
    # Extracting dataset ID, title, and modified date for logging and processing
    dataset_id = dataset["identifier"]
    title = dataset["title"]
    modified_date = dataset.get("modified")

    try:

        logger.info(f"Downloading dataset: {dataset_id} - {title}")

        # Assuming the first distribution is the one we want to download
        url = dataset["distribution"][0]["downloadURL"]

        if not url:
            logger.warning(f"No download URL found for dataset '{dataset_id}'. Skipping.")
            return None

        # Downloading the dataset and loading it into a DataFrame
        response = requests.get(url)
        response.raise_for_status()
        
        df = pd.read_csv(io.StringIO(response.text))

        # Normalizing column names to snake_case
        df.columns = [normalize_column_names_to_snake_case(col) for col in df.columns]

        # Creating output directory if it doesn't exist and saving the transformed dataset
        output_dir = os.path.join(OUTPUT_DIR_NAME, THEME)
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"{dataset_id}.csv")

        df.to_csv(output_file, index=False)

        logger.info(f"Dataset '{dataset_id}' transformed and saved to '{output_file}'")

        return dataset_id, modified_date
    
    except Exception as e:

        logger.error(f"Error processing dataset '{dataset_id}': {e}")
        return None
    
def main():
    logger.info("--------STARTING PIPELINE PROCESS--------")

    # Load Previous run data
    previous_run_data = load_previous_run_data()
    
    # Fetch new data from the API
    new_datasets = fetch_data_from_metastore()

    #Filter based on Hospital theme
    hospital_datasets = filter_datasets(new_datasets, THEME)

    #Check for new datasets
    new_datasets_to_download = [dataset for dataset in hospital_datasets if found_new_datasets(previous_run_data, dataset)]

    logger.info(f"Total new datasets to download: {len(new_datasets_to_download)}")

    # Process data in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(transform_data, new_datasets_to_download))

    # Updating the previous run data with the modified dates of the processed datasets
    for r in results:
        if r is not None:
            dataset_id, modified_date = r
            previous_run_data[dataset_id] = modified_date

    #Saving the current run data for future comparisons
    with open(PREVIOUS_RUN_DATA_FILE, "w") as file:
        json.dump(previous_run_data, file)
        logger.info("Previous run data updated and saved.")
    
    
    logger.info("--------PIPELINE PROCESS COMPLETED--------")


if __name__ == "__main__":
    main()
