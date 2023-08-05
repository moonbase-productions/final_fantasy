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

def extract_team_data(data: Dict) -> pd.DataFrame:
    try:
        if data.get('teams'):
            df = pd.DataFrame(data['teams'])
            df['idTeam'] = df['idTeam'].apply(lambda x: int(x) if x else 0)
            df['idLeague'] = df['idLeague'].apply(lambda x: int(x) if x else 0)
            df['intFormedYear'] = df['intFormedYear'].apply(lambda x: int(x) if x else 0)
            df = df[['idTeam', 'idLeague', 'strTeam', 'strTeamShort', 'intFormedYear', 'strStadiumDescription', 'intStadiumCapacity', 'strWebsite', 'strDescriptionEN']]
            return df
        else:
            return None
    except KeyError as e:
        logger.error(f"KeyError: {e} not found in data.")
        raise

def prepare_data(df: pd.DataFrame) -> List[Dict]:
    data_to_insert = []
    for index, row in df.iterrows():
        data_to_insert.append({
            'team_id': row['idTeam'],
            'league_id': row['idLeague'],
            'team_name': row['strTeam'],
            'team_short': row['strTeamShort'],
            'team_year_formed': row['intFormedYear'],
            'team_stadium_description': row['strStadiumDescription'],
            'team_stadium_capacity': row['intStadiumCapacity'],
            'team_website': row['strWebsite'],
            'team_description': row['strDescriptionEN']
        })
    return data_to_insert

def insert_data(supabase: Client, data: List[Dict]) -> None:
    try:
        supabase.table('api_assets').insert(data).execute()
    except Exception as e:
        logger.error(f"Failed to insert data into Supabase. Error: {e}")
        raise

def main():
    s_url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_USER_ROLE')
    supabase = create_client(s_url, key)

    response = supabase.table('api_leagues').select('league_id').execute()
    if response.error:
        logger.error(f"Failed to fetch data from Supabase. Error: {response.error}")
        return

    league_ids = [item['league_id'] for item in response.data]

    for league_id in league_ids:
        url = f"https://www.thesportsdb.com/api/v1/json/1/lookup_all_teams.php?id={league_id}"

        try:
            raw_data = fetch_data(url)
            df = extract_team_data(raw_data)
            if df is not None:
                data_to_insert = prepare_data(df)
                insert_data(supabase, data_to_insert)
                logger.info("Data inserted successfully into Supabase.")
        except Exception as e:
            logger.error(f"An error occurred during the execution of the script: {e}")

if __name__ == "__main__":
    main()
