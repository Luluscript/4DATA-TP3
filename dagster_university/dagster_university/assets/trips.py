import requests
from . import constants
from pathlib import Path
import duckdb
import os
from dagster._utils.backoff import backoff
from dagster_duckdb import DuckDBResource
from ..partitions import monthly_partition
from dagster import asset, AssetExecutionContext


# def taxi_trips_file() -> None:
#     """
#     Récupère les fichiers Parquet bruts des trajets en taxi.
#     Sourced from the NYC Open Data portal.
#     """
#     month_to_fetch = '2023-03'
#     raw_trips = requests.get(
#         f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
#     )

#     with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
#         output_file.write(raw_trips.content)




@asset(
    partitions_def=monthly_partition
)
def taxi_trips_file(context: AssetExecutionContext) -> None:
    """
    Récupère les fichiers Parquet bruts des trajets en taxi.
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)



@asset
def taxi_zones_file() -> None:
    """Télécharge les données des zones de taxi et les enregistre en CSV."""
    zones_files = requests.get("https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv")
    
    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(zones_files.content)




@asset(
    deps=["taxi_trips_file"],
    partitions_def=monthly_partition
)
def taxi_trips(database: DuckDBResource, context: AssetExecutionContext) -> None:
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]

    query = f"""
    create or replace table taxi_trips as
    select S
        VendorID as vendor_id,
        PULocationID as pickup_zone_id,
        DOLocationID as dropoff_zone_id,
        RatecodeID as rate_code_id,
        payment_type as payment_type,
        tpep_dropoff_datetime as dropoff_datetime,
        tpep_pickup_datetime as pickup_datetime,
        trip_distance as trip_distance,
        passenger_count as passenger_count,
        total_amount as total_amount,
        '{partition_date_str}' as partition_date  -- Ajout de la colonne partition_date
    from 'data/raw/taxi_trips_{month_to_fetch}.parquet'
    """

    with database.get_connection() as conn:
        conn.execute(query)




@asset(
deps=["taxi_zones_file"]
)
def taxi_zones(database: DuckDBResource) -> None:
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

    with database.get_connection() as conn:
        conn.execute(query)