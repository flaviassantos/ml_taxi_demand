import os
from dotenv import load_dotenv

from src.paths import PARENT_DIR
from src.feature_store_api import FeatureGroupConfig, FeatureViewConfig

# load key-value pairs from .env file located in the parent directory
load_dotenv(PARENT_DIR / '.env')


try:
    HOPSWORKS_PROJECT_NAME = os.environ['HOPSWORKS_PROJECT_NAME']
    HOPSWORKS_API_KEY = os.environ['HOPSWORKS_API_KEY']
    SAVE_FEATURE_GROUP = os.environ['SAVE_FEATURE_GROUP']
except:
    raise Exception('Create an .env file on the project root with the HOPSWORKS_API_KEY and HOPSWORKS_PROJECT_NAME')

# TODO: get version dinamically
FEATURE_GROUP_METADATA = FeatureGroupConfig(
    name='time_series_hourly_feature_group',
    version=1,
    description='Feature group with hourly time-series data of historical taxi rides',
    primary_key=['pickup_location_id', 'pickup_ts'],
    event_time='pickup_ts',
    online_enabled=False,
)

FEATURE_VIEW_METADATA = FeatureViewConfig(
    name='time_series_hourly_feature_view',
    version=1,
    feature_group=FEATURE_GROUP_METADATA,
)

FEATURE_GROUP_PREDICTIONS_METADATA = FeatureGroupConfig(
    name='model_predictions_feature_group',
    version=1,
    description="Predictions generate by our production model",
    primary_key = ['pickup_location_id', 'pickup_ts'],
    event_time='pickup_ts',
)

# number of iterations we want Optuna to pefrom to find the best hyperparameters
N_HYPERPARAMETER_SEARCH_TRIALS = 1

# number of historical values our model needs to generate predictions
N_FEATURES = 24 * 28

# maximum Mean Absolute Error we allow our production model to have
MAX_MAE = 9.0

MODEL_NAME = "lightgbm_taxi_demand_predictor"

STEP_SIZE = 23

CUTOFF_DATE = 120

PREVIOUS_YEAR = 7*52