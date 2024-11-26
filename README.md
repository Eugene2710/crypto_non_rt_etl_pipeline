# Crypto Non-RT ETL Pipeline

Powerful Non-RT Data Pipeline that siphons data from multiple crypto data source (e.g QuickNode, Bloomberg) periodically at a high throughput, and ingest it into a database of choice (Postgres).

Databases and Data sources are easily pluggable and change-able

QuickNode provides raw data from Ethereum / Polygon nodes, but it doesn't give us flexibility to extract data from other data sources, normalize the data into a clean format, and transform it with data from other data sources into business-level data ready to be used.

This data pipeline allows us to do that.

REMINDER: to run this on your end, you need a chainstack account, 
or quicknode account (change the environment variables accordingly if this is used)

## MIRO Architecture 
 
![image](./images/architecture.png)

## Project Setup

### Create venv and install dependencies

```commandline
poetry shell
poetry install --no-root
```

### Setup postgresql@14

Project requires postgresql@14.

Spin up a local instance of postgresql@14

```
brew services start postgresql@14
```

Connect to the local instance, and create a new database

```commandline
psql -d postgres
CREATE DATABASE quick_node;
```

Finally, use alembic to create the tables

```
alembic upgrade head
```

## Running the data pipeline to extract and load ethereum block information from quick node, into postgres database

Step 1: Populate .env file with environment variables

Step 2: Run the pipeline

```commandline
export PYTHONPATH=.
python src/chain_stack_eth_block_etl_pipeline.py
```

### Streamlit
```commandline
ngrok http --url=eminently-accurate-lacewing.ngrok-free.app 8501
export PYTHONPATH=.
streamlit run client/streamlit_app.py
```
Visit [this link](https://eminently-accurate-lacewing.ngrok-free.app) for a live demo of the streamlit
![image](./images/crypto_non_rt_etl_pipeline_streamlit.png)
This streamlit app allows the querying of database and visualizing the data

### Airflow
Refer to [this repository](https://github.com/Eugene2710/crypto_non_rt_etl_pipeline_dag) for ETL pipeline schedule in dag files

![image](./images/airflow_crypto_non_rt_etl_pipeline_dag.png)

### Docker Compose
```commandline
docker login -u "docker_username"
docker-compose up --build --force-recreate --no-deps 
```
![image](./images/docker_compose_successful_run.png)

## TODOs:
- Integration test the rest of the DAOs
- Unit Test ETLPipeline Service Level class to DTO class
- Explore other types of data to be extracted from Chainstack
- Explore use cases of data