from typing import Optional, List
from dataclasses import dataclass

import hsfs
import hopsworks
import pandas as pd

import src.config as config
from src.paths import DATA_CACHE_DIR
from src.logger import get_logger

logger = get_logger()

@dataclass
class FeatureGroupConfig:
    """A feature group is a logical table of features. In practice,feature groups are stored in e.g. OLTP databases such as Postgresql"""
    name: str
    version: int
    description: str
    primary_key: List[str]
    event_time: str
    online_enabled: Optional[bool] = True

@dataclass
class FeatureViewConfig:
    """A feature view is a selection of features (and labels) from one or more feature groups."""
    name: str
    version: int
    feature_group: FeatureGroupConfig

def get_feature_store() -> hsfs.feature_store.FeatureStore:
    """Connects to Hopsworks and returns a pointer to the feature store

    Returns:
        hsfs.feature_store.FeatureStore: pointer to the feature store
    """
    project = hopsworks.login(
        project=config.HOPSWORKS_PROJECT_NAME,
        api_key_value=config.HOPSWORKS_API_KEY
    )
    return project.get_feature_store()

# TODO: remove this function, and use get_or_create_feature_group instead
def get_feature_group(
    name: str,
    version: Optional[int] = 1
    ) -> hsfs.feature_group.FeatureGroup:
    """Connects to the feature store and returns a pointer to the given
    feature group `name`

    Args:
        name (str): name of the feature group
        version (Optional[int], optional): _description_. Defaults to 1.

    Returns:
        hsfs.feature_group.FeatureGroup: pointer to the feature group
    """
    return get_feature_store().get_feature_group(
        name=name,
        version=version,
    )

def get_or_create_feature_group(
    feature_group_metadata: FeatureGroupConfig
) -> hsfs.feature_group.FeatureGroup:
    """Connects to the feature store and returns a pointer to the given
    feature group `name`

    Args:
        name (str): name of the feature group
        version (Optional[int], optional): _description_. Defaults to 1.

    Returns:
        hsfs.feature_group.FeatureGroup: pointer to the feature group
    """
    project = get_feature_store()
    feature_group = project.get_or_create_feature_group(
        name=feature_group_metadata.name,
        version=feature_group_metadata.version,
        description=feature_group_metadata.description,
        primary_key=feature_group_metadata.primary_key,
        event_time=feature_group_metadata.event_time,
        online_enabled=feature_group_metadata.online_enabled
        
    )
    return feature_group

def get_or_create_feature_view(
    feature_view_metadata: FeatureViewConfig
) -> hsfs.feature_view.FeatureView:
    """"""

    feature_store = get_feature_store()

    feature_group = feature_store.get_feature_group(
        name=feature_view_metadata.feature_group.name,
        version=feature_view_metadata.feature_group.version
    )

    try:
        feature_store.create_feature_view(
            name=feature_view_metadata.name,
            version=feature_view_metadata.version,
            query=feature_group.select_all()
        )
    except:
        logger.info("Feature view already exists, skipping creation.")
    
    feature_store = get_feature_store()
    feature_view = feature_store.get_feature_view(
        name=feature_view_metadata.name,
        version=feature_view_metadata.version,
    )

    return feature_view


def feature_group_insert(ts_data: pd.DataFrame, file_name: str = 'feature_group.parquet'):
    """"""
    if config.SAVE_FEATURE_GROUP == 'local':
        local_file = DATA_CACHE_DIR / file_name
        ts_data.to_parquet(local_file)
        logger.info(f'Saved feature group to local file at {local_file}')
    elif config.SAVE_FEATURE_GROUP == 'feature_store':
        from src.feature_store_api import get_or_create_feature_group
        
        logger.info('Getting pointer to the feature group we wanna save data to')
        feature_group = get_or_create_feature_group(config.FEATURE_GROUP_METADATA)

        logger.info('Starting job to insert data into feature group...')
        feature_group.insert(ts_data, write_options={"wait_for_job": False})
        # feature_group.insert(ts_data, write_options={"start_offline_backfill": False})
        
        logger.info('Finished job to insert data into feature group')
    else:
        logger.info("Create an .env file on the project root with the SAVE_FEATURE_GROUP. Values accepted: 'local' or 'feature_store'.")
        pass