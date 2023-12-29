.PHONE: init backfill features

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