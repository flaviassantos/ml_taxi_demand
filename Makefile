.PHONE: init backfill features training

include .env

# downloads Poetry and installs all dependencies from pyproject.toml
init:
	curl -sSL https://install.python-poetry.org | python3 -
	poetry install

# generates new batch of features and stores them in the feature store
features:
	poetry run python scripts/feature_pipeline.py

# backfills the feature store with historical data
backfill:
	poetry run python scripts/backfill_feature_group.py

# trains a new model and stores it in the model registry
training:
	env $(cat .env | grep -v '^#' | sed '/^$$/d' | awk -F= '{print $$1}') poetry run python scripts/training_pipeline.py --local_path_features_and_target=${LOCAL_PATH_FEATURES}