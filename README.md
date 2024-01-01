# Machine Learning - Taxi Demand

Building a complete ML service, from A to Z.

## Serverless MLOps tools 🛠
- Hopsworks as our Feature Store
- CometML for model registry and management
- GitHub Actions to schedule and run jobs
  

## Wanna see it in action?

- [Live Dashboard with model predictions](https://xxxxxx.streamlit.app/)

<div align="center">
    <sub>Let's connect 🤗</sub>
    <br />
    <a href="www.linkedin.com/in/flaviasouzasantos">LinkedIn</a> •
    <a href="https://.substack.com/">Newsletter</a>
<br />
</div>

## Quick setup

1. Install [Python Poetry](https://python-poetry.org/)
    ```bash
    curl -sSL https://install.python-poetry.org | python3 -
    ```
  - Extra set up for Mac M1/M2 chips

    ```bash
    $ brew update
    $ brew install libomp librdkafka
    ```

2. cd into the project folder and run
        
    ```bash
    $ poetry install
    ```

3. Activate the virtual env that you just created with
    ```bash
    $ poetry shell
    ```

4. Open free accounts at Hopsworks and CometML and copy your project names and API keys in an .env file
    ```bash
    $ cp .env.sample .env
    # paste your values there
    ```

5. Backfill the feature group with historical data
    ```bash
    $ make backfill
    ```

6. Run the training pipeline
    ```bash
    $ make training
    ```

7. Run the feature pipeline for the last hour
    ```bash
    $ make features
    ```

9. Run the inference pipeline for the last hour
    ```bash
    $ make inference
    ```
