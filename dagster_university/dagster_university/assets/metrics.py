from dagster import asset
import matplotlib.pyplot as plt
import geopandas as gpd
import duckdb
import os
from . import constants
from dagster._utils.backoff import backoff
import pandas as pd



@asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats() -> None:
    """
    Calcule les statistiques des trajets en taxi pour Manhattan et les stocke au format GeoJSON.
    """
    query = """
        SELECT
            zones.zone,
            zones.borough,
            zones.geometry,
            COUNT(1) AS num_trips
        FROM trips
        LEFT JOIN zones ON trips.pickup_zone_id = zones.zone_id
        WHERE borough = 'Manhattan' AND geometry IS NOT NULL
        GROUP BY zone, borough, geometry
    """

    conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())


@asset(
    deps=["manhattan_stats"]
)
def manhattan_map() -> None:
    """
    Génère une carte des trajets en taxi à Manhattan et l'enregistre sous forme d'image.
    """
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(column="num_trips", cmap="plasma", legend=True, ax=ax, edgecolor="black")
    ax.set_title("Nombre de trajets par zone de taxi à Manhattan")

    ax.set_xlim([-74.05, -73.90])
    ax.set_ylim([40.70, 40.82])
    
    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(fig)




# Création de l'asset trips_by_week
@asset(deps=["taxi_trips"])
def trips_by_week():
    """Agrège les trajets par semaine et stocke les résultats dans un fichier CSV."""

    # Requête pour agréger les trajets par semaine
    query = """
        SELECT 
            DATE_TRUNC('week', pickup_datetime) AS period,
            COUNT(*) AS num_trips,
            SUM(passenger_count) AS passenger_count,
            SUM(total_amount) AS total_amount,
            SUM(trip_distance) AS trip_distance
        FROM trips
        WHERE pickup_datetime BETWEEN '2023-03-01' AND '2023-03-31'
        GROUP BY 1
        ORDER BY 1;
    """
    conn = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE"),
        },
        max_retries=10,
    )

    # Exécuter la requête et récupérer le DataFrame
    trips_weekly = conn.execute(query).fetch_df()

    # Conversion de la colonne 'period' en format string (dimanche de la semaine)
    trips_weekly["period"] = pd.to_datetime(trips_weekly["period"]).dt.strftime("%Y-%m-%d")

    with open(constants.TRIPS_BY_WEEK_FILE_PATH, "w", encoding="utf-8") as output_file:
        output_file.write(trips_weekly.to_csv())
