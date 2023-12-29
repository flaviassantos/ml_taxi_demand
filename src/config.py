import os
from dotenv import load_dotenv

from src.paths import PARENT_DIR
from src.feature_store_api import FeatureGroupConfig, FeatureViewConfig

# load key-value pairs from .env file located in the parent directory
load_dotenv(PARENT_DIR / '.env')


try:
    HOPSWORKS_PROJECT_NAME = os.environ['HOPSWORKS_PROJECT_NAME']
    HOPSWORKS_API_KEY = os.environ['HOPSWORKS_API_KEY']
except:
    raise Exception('Create an .env file on the project root with the HOPSWORKS_API_KEY and HOPSWORKS_PROJECT_NAME')


FEATURE_GROUP_METADATA = FeatureGroupConfig(
    name='time_series_hourly_feature_group',
    version=1,
    description='Feature group with hourly time-series data of historical taxi rides',
    primary_key=['pickup_location_id', 'pickup_ts'],
    event_time='pickup_ts',
    online_enabled=True,
)