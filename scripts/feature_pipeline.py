from datetime import datetime, timedelta
from argparse import ArgumentParser
from typing import Optional
from pathlib import Path

import pandas as pd

from src import config

from src.data import (
    fetch_ride_events_from_data_warehouse,
    load_raw_data,
    transform_raw_data_into_ts_data,
    transform_ts_data_into_features_and_target,
)
from src.feature_store_api import feature_group_insert, get_or_create_feature_group

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
    
    if config.SAVE_FEATURE_GROUP == 'local':
        features = _transform_raw_data_into_ts_data(date)
        feature_group_insert(features)
    elif config.SAVE_FEATURE_GROUP == 'feature_store':
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

        # get a pointer to the feature group we wanna write to
        logger.info('Getting pointer to the feature group we wanna save data to')
        feature_group = get_or_create_feature_group(config.FEATURE_GROUP_METADATA)

        # start a job to insert the data into the feature group
        # we wait, to make sure the job is finished before we exit the script, and
        # the inference pipeline can start using the new data
        logger.info('Starting job to insert data into feature group...')
        feature_group.insert(ts_data, write_options={"wait_for_job": False})
        # feature_group.insert(ts_data, write_options={"start_offline_backfill": False})
        
        logger.info('Finished job to insert data into feature group')
    else:
        logger.info("Create an .env file on the project root with the SAVE_FEATURE_GROUP. Values accepted: 'local' or 'feature_store'.")
        pass
    


def _transform_raw_data_into_ts_data(current_date: datetime) -> pd.DataFrame:
    """
     fetch raw ride events from the datawarehouse for the last 28 days
     we fetch the last 28 days, instead of the last hour only, to add redundancy
     to the feature_pipeline. This way, if the pipeline fails for some reason,
     we can still re-write data for that missing hour in a later run.
    """
    from_date=(current_date - timedelta(days=config.CUTOFF_DATE))
    to_date=current_date

    from_date_ = from_date - timedelta(days=config.PREVIOUS_YEAR)
    to_date_ = to_date - timedelta(days=config.PREVIOUS_YEAR)
    logger.info(f'Fetching ride events from {from_date} to {to_date}')
    
    if (from_date_.year == to_date_.year) and (from_date_.month == to_date_.month):
    # download 1 file of data only
        rides = load_raw_data(year=from_date_.year, months=from_date_.month)
        rides = rides[rides.pickup_datetime >= from_date_]
        rides = rides[rides.pickup_datetime < to_date_]

    else:
        # download 2 files from website
        rides = load_raw_data(year=from_date_.year, months=from_date_.month)
        rides = rides[rides.pickup_datetime >= from_date_]
        rides_2 = load_raw_data(year=to_date_.year, months=to_date_.month)
        rides_2 = rides_2[rides_2.pickup_datetime < to_date_]
        rides = pd.concat([rides, rides_2])

    # shift the pickup_datetime back 1 year ahead, to simulate production data
    # using its 7*52-days-ago value
    rides['pickup_datetime'] += timedelta(days=config.PREVIOUS_YEAR)
    rides.sort_values(by=['pickup_location_id', 'pickup_datetime'], inplace=True)

    # transform raw data into time-series data by aggregating rides per
    # pickup location and hour
    logger.info('Transforming raw data into time-series data')
    ts_data = transform_raw_data_into_ts_data(rides)

    # add new column with the timestamp in Unix seconds
    logger.info('Adding column `pickup_ts` with Unix seconds...')
    # ts_data['pickup_hour'] = pd.to_datetime(ts_data['pickup_hour'], utc=True)    
    ts_data['pickup_ts'] = ts_data['pickup_hour'].astype(int) // 10**6
    
    features, _ = transform_ts_data_into_features_and_target(
        ts_data,
        n_features=config.N_FEATURES, # one month
        step_size=config.STEP_SIZE,
    )
    
    return features


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
    # current_date = pd.to_datetime(datetime.strptime('2023-09-03 00:00:00', '%Y-%m-%d %H:%M:%S')).floor('H')
    
    run(current_date)
