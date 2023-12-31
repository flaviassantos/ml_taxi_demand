from datetime import datetime, timedelta
from argparse import ArgumentParser
from typing import Optional
from pathlib import Path

import pandas as pd

from src import config

from src.data import (
    fetch_ride_events_from_data_warehouse,
    transform_raw_data_into_ts_data,
)
from src.feature_store_api import feature_group_insert

from src.logger import get_logger

logger = get_logger()


def run(date: datetime):
    """_summary_

    Args:
        date (datetime): _description_

    Returns:
        _type_: _description_
    """
    logger.info('Fetching raw data from data warehouse')

    # fetch raw ride events from the datawarehouse for the last 28 days
    # we fetch the last 28 days, instead of the last hour only, to add redundancy
    # to the feature_pipeline. This way, if the pipeline fails for some reason,
    # we can still re-write data for that missing hour in a later run.
    rides = fetch_ride_events_from_data_warehouse(
        from_date=(date - timedelta(days=28)),
        to_date=date
    )

    # transform raw data into time-series data by aggregating rides per
    # pickup location and hour
    logger.info('Transforming raw data into time-series data')
    ts_data = transform_raw_data_into_ts_data(rides)

    # add new column with the timestamp in Unix seconds
    logger.info('Adding column `pickup_ts` with Unix seconds...')
    ts_data['pickup_hour'] = pd.to_datetime(ts_data['pickup_hour'], utc=True)    
    ts_data['pickup_ts'] = ts_data['pickup_hour'].astype(int) // 10**6
    
    feature_group_insert(ts_data)


if __name__ == '__main__':
    # TODO: try out different dates from command line
    # parse command line arguments
    parser = ArgumentParser()
    parser.add_argument('--datetime',
                        type=lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M:%S'),
                        help='Datetime argument in the format of YYYY-MM-DD HH:MM:SS')
    args = parser.parse_args()

    # if args.datetime was provided, use it as the current_date, otherwise
    # use the current datetime in UTC
    if args.datetime:
        current_date = pd.to_datetime(args.datetime)
    else:
        current_date = pd.to_datetime(datetime.utcnow()).floor('H')
    
    logger.info(f'Running feature pipeline for {current_date=}')
    run(current_date)
