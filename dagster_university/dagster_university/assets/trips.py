import requests
from . import constants
from pathlib import Path
import duckdb
import os
from dagster import asset
from dagster._utils.backoff import backoff


def taxi_trips_file() -> None:
    """
    Récupère les fichiers Parquet bruts des trajets en taxi.
    Sourced from the NYC Open Data portal.
    """
    month_to_fetch = '2023-03'
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)




@asset
def taxi_trips_file() -> None:
    """
    Récupère les fichiers Parquet bruts des trajets en taxi.
    """
    month_to_fetch = '2023-03'
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)


# 📌 Création de l'asset taxi_zones_file
@asset
def taxi_zones_file() -> None:
    """Télécharge les données des zones de taxi et les enregistre en CSV."""
    zones_files = requests.get("https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv")
    
    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(zones_files.content)





@asset(
deps=["taxi_trips_file"]
)
def taxi_trips() -> None:
    """
    Le jeu de données brut des trajets en taxi, chargé dans une base de données DuckDB.
    """
    query = """
        CREATE OR REPLACE TABLE trips AS (
          SELECT
            VendorID AS vendor_id,
            PULocationID AS pickup_zone_id,
            DOLocationID AS dropoff_zone_id,
            RatecodeID AS rate_code_id,
            payment_type AS payment_type,
            tpep_dropoff_datetime AS dropoff_datetime,
            tpep_pickup_datetime AS pickup_datetime,
            trip_distance AS trip_distance,
            passenger_count AS passenger_count,
            total_amount AS total_amount
          FROM 'data/raw/taxi_trips_2023-03.parquet'
        );
    """

    conn = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE"),
        },
        max_retries=10,
    )
    conn.execute(query)



@asset(
deps=["taxi_zones_file"]
)
def taxi_zones() -> None:
    """
    Le jeu de données brut des zones en taxi, chargé dans une base de données DuckDB.
    """
    query = """
        CREATE OR REPLACE TABLE zones AS (
          SELECT
            LocationID AS zone_id,
            zone AS zone,
            borough AS borough,
            the_geom AS geometry,  
          FROM 'data/raw/taxi_zones.csv'
        );
    """

    conn = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE"),
        },
        max_retries=10,
    )
    conn.execute(query)