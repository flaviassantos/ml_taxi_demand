# import os
from datetime import datetime

# from dotenv import load_dotenv
import pandas as pd

from src.data import load_raw_data, transform_raw_data_into_ts_data
from src.config import FEATURE_GROUP_METADATA 
from src.feature_store_api import feature_group_insert, get_or_create_feature_group
from src.logger import get_logger

logger = get_logger()


def get_historical_rides() -> pd.DataFrame:
    """
    Download historical rides from the NYC Taxi dataset
    """
    # TODO: implement new backfill, maybe get from_year from the command line
    from_year = 2022

    to_year = datetime.now().year
    print(f'Downloading raw data from {from_year} to {to_year}')

    rides = pd.DataFrame()
    for year in range(from_year, to_year+1):
        
        # download data for the whole year
        rides_one_year = load_raw_data(year)
        
        # append rows
        rides = pd.concat([rides, rides_one_year])

    return rides


def run():

    logger.info('Fetching raw data from data warehouse')
    rides = get_historical_rides()

    logger.info('Transforming raw data into time-series data')
    ts_data = transform_raw_data_into_ts_data(rides)

    # add new column with the timestamp in Unix seconds
    ts_data['pickup_hour'] = pd.to_datetime(ts_data['pickup_hour'], utc=True)    
    ts_data['pickup_ts'] = ts_data['pickup_hour'].astype(int) // 10**6 # Unix milliseconds
    
    # TODO: fix proper backfill on database
    feature_group_insert(ts_data)

if __name__ == '__main__':
    run()