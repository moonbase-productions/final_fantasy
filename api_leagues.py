import os
import requests
import pandas as pd
import logging
from supabase import create_client, Client
from typing import List, Dict
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Set up file logging
file_handler = logging.FileHandler('log.csv')
formatter = logging.Formatter('%(asctime)s, %(levelname)s, %(message)s')
file_handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(file_handler)


def fetch_data(url: str) -> Dict:
    try:
        logger.info("API call initiated.")
        start_time = datetime.now()
        response = requests.get(url)
        response.raise_for_status()  # Will raise an HTTPError if the response was unsuccessful
        end_time = datetime.now()
        logger.info(f"API call terminated. Duration: {end_time - start_time}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data from {url}. Error: {e}")
        raise


def extract_league_data(data: Dict) -> pd.DataFrame:
    try:
        df = pd.DataFrame(data['leagues'])
        df = df[['idLeague', 'strLeague', 'strSport']]
        df['idLeague'] = df['idLeague'].astype(int)
        return df
    except KeyError as e:
        logger.error(f"KeyError: {e} not found in data.")
        raise


def prepare_data(df: pd.DataFrame) -> List[Dict]:
    data_to_insert = []
    for index, row in df.iterrows():
        data_to_insert.append({
            'league_id': row['idLeague'],
            'league_name': row['strLeague'],
            'league_sport': row['strSport']
        })
    return data_to_insert


def insert_data(supabase: Client, data: List[Dict]) -> None:
    try:
        supabase.table('api_leagues').insert(data).execute()
    except Exception as e:
        logger.error(f"Failed to insert data into Supabase. Error: {e}")
        raise


def main():
    s_url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_USER_ROLE')
    url = 'https://www.thesportsdb.com/api/v1/json/3/all_leagues.php'

    try:
        raw_data = fetch_data(url)
        df = extract_league_data(raw_data)

        if df is not None:
            data_to_insert = prepare_data(df)

            supabase = create_client(s_url, key)
            insert_data(supabase, data_to_insert)
            logger.info("Data inserted successfully into Supabase.")

    except Exception as e:
        logger.error(f"An error occurred during the execution of the script: {e}")


if __name__ == "__main__":
    main()
