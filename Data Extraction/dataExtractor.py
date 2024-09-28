import requests
import json
import gzip
import shutil
import os
from io import BytesIO
import argparse
from dotenv import load_dotenv

load_dotenv()

# Constants
S3_BUCKET_URL = os.getenv("S3_BUCKET_URL")

# Function to download and extract gzipped JSON data
def download_gzip_and_write_to_json(file_name):
    if os.path.isfile(f"{file_name}.json"):
        return False

    remote_file = f"{S3_BUCKET_URL}/{file_name}.json.gz"
    response = requests.get(remote_file, stream=True)

    if response.status_code == 200:
        gzip_bytes = BytesIO(response.content)
        with gzip.GzipFile(fileobj=gzip_bytes, mode="rb") as gzipped_file:
            with open(f"{file_name}.json", 'wb') as output_file:
                shutil.copyfileobj(gzipped_file, output_file)
            print(f"{file_name}.json written")
        return True
    elif response.status_code == 404:
        # File not found
        return False
    else:
        print(f"Failed to download {file_name}")
        return False

# Function to download esports data files
def download_esports_files(league):
    directory = f"{league}/esports-data"
    if not os.path.exists(directory):
        os.makedirs(directory)

    esports_data_files = ["leagues", "tournaments", "players", "teams", "mapping_data"]
    for file_name in esports_data_files:
        download_gzip_and_write_to_json(f"{directory}/{file_name}")

# Function to download game data files for a specific year
def download_games(league, year):
    local_mapping_file = f"{league}/esports-data/mapping_data.json"
    
    # Check if mapping data exists
    if not os.path.exists(local_mapping_file):
        print(f"Mapping data not found for {league}")
        return

    # Read mappings data
    with open(local_mapping_file, "r") as json_file:
        mappings_data = json.load(json_file)

    local_directory = f"{league}/games/{year}"
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    game_counter = 0

    # Iterate over mapping data and download each game file
    for esports_game in mappings_data:
        s3_game_file = f"{league}/games/{year}/{esports_game['platformGameId']}"
        response = download_gzip_and_write_to_json(s3_game_file)
        
        if response:
            game_counter += 1
            if game_counter % 10 == 0:
                print(f"----- Processed {game_counter} games")

if __name__ == "__main__":
    # Argument parsing setup
    parser = argparse.ArgumentParser(description="Download esports data and game data from S3.")
    parser.add_argument('--league', type=str, required=True, help="The league to download (e.g., 'game-changers', 'vct-challengers', 'vct-international').")
    parser.add_argument('--year', type=int, required=True, help="The year of the game data to download (e.g., 2022, 2023, 2024).")

    # Parse arguments
    args = parser.parse_args()

    # Download esports and game data based on the arguments
    download_esports_files(args.league)
    download_games(args.league, args.year)
