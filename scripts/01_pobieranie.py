import os
import urllib.request

# Project configuration and constants
URL = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"
DATA_DIR = "data/raw"
FILE_PATH = os.path.join(DATA_DIR, "pp-complete.csv")

def download_data():
    # Initialize the target directory if it does not exist
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        print(f"Directory created: {DATA_DIR}")

    # Check for existing local data to avoid redundant downloads
    if os.path.exists(FILE_PATH):
        size_gb = os.path.getsize(FILE_PATH) / (1024**3)
        print(f"Data already present: {FILE_PATH} ({size_gb:.2f} GB). Skipping download.")
        return

    # Proceed with data ingestion if file is missing
    try:
        print("--- INGESTION IN PROGRESS ---")
        print("Downloading large dataset, this may take a few minutes...")
        
        # Execute the download process from the source URL
        urllib.request.urlretrieve(URL, FILE_PATH)
        
        print("Download completed")
    except Exception as e:
        print(f"An error occurred during ingestion: {e}")
        print("Please attempt manual download from the source URL provided above.")

if __name__ == "__main__":
    download_data()