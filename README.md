# Machine Learning - Taxi Demand

Building a complete ML service, from A to Z.

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

2. cd into the project folder and run
        ```bash
        $ poetry install
        ```

3. Activate the virtual env that you just created with
    ```bash
    $ poetry shell
    ```

3. Open free accounts at Hopsworks and CometML and copy your project names and API keys in an .env file
    ```bash
    $ cp .env.sample .env
    # paste your values there
    ```

4. Backfill the feature group with historical data
    ```bash
    $ make backfill
    ```

5. Run the feature pipeline for the last hour
    ```bash
    $ make features
    ```



