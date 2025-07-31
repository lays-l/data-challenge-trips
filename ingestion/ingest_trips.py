import pandas as pd
import reverse_geocoder as rg
import re
import os
import logging
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import argparse
import time

# ------------------------------------------------------------
# 1. Initial setup
# ------------------------------------------------------------
# Configure log level and message format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
# Load environment variables from .env file (user, password, etc.)
load_dotenv()

GEO_CACHE = {}  # fora do loop

# ------------------------------------------------------------
# 2. parse_point function: convert WKT or "lat,lon" string into (lat, lon) tuple
# ------------------------------------------------------------
def parse_point(coord_str):
    """
    Take a coordinate string and return (latitude, longitude).
    Supports:
      - WKT: "POINT(<lon> <lat>)"
      - CSV: "<lat>,<lon>"
    Returns None if the string is empty or invalid.
    """
    if pd.isna(coord_str) or not coord_str:
        return None

    # WKT case: extract lon and lat via regex
    match = re.match(r'POINT\s*\(([-\d\.]+)\s+([-\d\.]+)\)', coord_str)
    if match:
        lon, lat = map(float, match.groups())
        return lat, lon

    # "lat,lon" case
    if ',' in coord_str:
        lat, lon = map(float, coord_str.split(','))
        return lat, lon

    logging.warning("Unrecognized coordinate format: %s", coord_str)
    return None


# ------------------------------------------------------------
# 3. enrich_batch function: enrich a DataFrame with city, country, and latitude/longitude
# ------------------------------------------------------------
# ------------------------------------------------------------
# 3. enrich_batch function with global caching
# ------------------------------------------------------------
def enrich_batch(df, source_col, prefix):
    """
    For each value in df[source_col]:
      - parse into pt = (lat, lon)
      - if pt in GEO_CACHE, reuse GEO_CACHE[pt]
      - else call rg.search once and store in GEO_CACHE
      - populate {prefix}_city, country, latitude, longitude
    """
    city_col    = f"{prefix}_city"
    country_col = f"{prefix}_country"
    lat_col     = f"{prefix}_latitude"
    lon_col     = f"{prefix}_longitude"

    # initialize empty columns
    df[city_col]    = None
    df[country_col] = None
    df[lat_col]     = None
    df[lon_col]     = None

    for idx, coord_str in enumerate(df[source_col]):
        pt = parse_point(coord_str)
        if not pt:
            continue

        # use or populate global cache
        if pt in GEO_CACHE:
            res = GEO_CACHE[pt]
        else:
            # single-point batch, retorna lista com 1 item
            res = rg.search([pt], mode=1)[0]
            GEO_CACHE[pt] = res

        # preenche
        lat, lon = pt
        df.at[idx, city_col]    = res['name']
        df.at[idx, country_col] = res['cc']
        df.at[idx, lat_col]     = lat
        df.at[idx, lon_col]     = lon


# ------------------------------------------------------------
# 4. main function: orchestrate reading, enrichment, and loading into Snowflake
# ------------------------------------------------------------
def main(csv_file):
    """
    1) Connect to Snowflake using credentials in .env
    2) Ensure the TRIPS table exists
    3) Read the CSV in chunks (chunksize=100k)
    4) For each chunk:
         a) Rename 'datetime' to 'departure_time
         b) Enrich origin_coord and destination_coord
         c) Normalize column names to uppercase and reorder
         d) Bulk-insert using write_pandas
         e) Clear the internal stage
    """
    # 4.1 Connect to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
    )
    cs = conn.cursor()

    # 4.2 Create the trips table if it doesn't already exist
    cs.execute("""
        CREATE TABLE IF NOT EXISTS TRIPS(
            region                VARCHAR,
            origin_coord          VARCHAR,
            destination_coord     VARCHAR,
            departure_time        TIMESTAMP,
            datasource            VARCHAR,
            origin_city           VARCHAR,
            origin_country        VARCHAR,
            origin_latitude       FLOAT,
            origin_longitude      FLOAT,
            destination_city      VARCHAR,
            destination_country   VARCHAR,
            destination_latitude  FLOAT,
            destination_longitude FLOAT
        )
    """)
    cs.close()

    # Fixed list of columns in the correct order for write_pandas
    expected_cols = [
        "REGION",
        "ORIGIN_COORD",
        "DESTINATION_COORD",
        "DEPARTURE_TIME",
        "DATASOURCE",
        "ORIGIN_CITY",
        "ORIGIN_COUNTRY",
        "ORIGIN_LATITUDE",
        "ORIGIN_LONGITUDE",
        "DESTINATION_CITY",
        "DESTINATION_COUNTRY",
        "DESTINATION_LATITUDE",
        "DESTINATION_LONGITUDE"
    ]

    # 4.3 Process file in chunks
    reader = pd.read_csv(csv_file, chunksize=50_000)
    for i, chunk in enumerate(reader, start=1):
        logging.info("Processing chunk %d: %d rows", i, len(chunk))

        # a) Rename datetime column
        if 'datetime' in chunk.columns:
            chunk.rename(columns={'datetime': 'departure_time'}, inplace=True)

        # b) Enrich coordinates
        t0 = time.perf_counter()
        enrich_batch(chunk, "origin_coord", "origin")
        enrich_batch(chunk, "destination_coord", "destination")
        logging.info("Chunk %d enrichment took %.1f s", i, time.perf_counter() - t0)

        # c) Uppercase column names, reorder e reset index
        chunk.columns = chunk.columns.str.upper()
        chunk = chunk.reindex(columns=expected_cols)
        chunk.reset_index(drop=True, inplace=True)

        # d) Bulk insert via write_pandas
        success, nchunks, nrows, _ = write_pandas(
            conn, chunk,
            table_name='TRIPS',
            chunk_size=25_000,
            parallel=4
        )
        if not success:
            logging.error("Failed to insert chunk %d", i)
        else:
            logging.info("Chunk %d inserted: %d rows in %d operations", i, nrows, nchunks)

        # e) Clear the internal stage so files don’t accumulate between chunks
        with conn.cursor() as cs:
            cs.execute("REMOVE @%TRIPS")  
            logging.info("Stage TRIPS cleared")

    # 4.4 Close the connection when done
    conn.close()
    logging.info("Pipeline completed successfully!")


# ------------------------------------------------------------
# 5. Entry point: argument parsing and main invocation
# ------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enrich coordinates and bulk-load CSV into Snowflake"
    )
    parser.add_argument(
        "--file", required=True,
        help="Path to the input CSV file"
    )
    args = parser.parse_args()

    logging.info("Starting pipeline for file: %s", args.file)
    main(args.file)
