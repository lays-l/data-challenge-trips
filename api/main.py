from fastapi import FastAPI, Query, UploadFile, File
from typing import Optional
import os
import logging
import tempfile
import snowflake.connector
from dotenv import load_dotenv
from ingestion.ingest_trips import main as enrich_and_save_to_snowflake

# ------------------------------------------------------------
# 1. Initial setup
# ------------------------------------------------------------
# Configure log level and message format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
# Load environment variables from .env (Snowflake credentials, etc.)
load_dotenv()

app = FastAPI()


# ------------------------------------------------------------
# 2. get_connection: create and return a Snowflake connection
# ------------------------------------------------------------
def get_connection():
    """
    Establish a connection to Snowflake using environment variables.
    Returns a Snowflake Connector connection object.
    """
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
    )


# ------------------------------------------------------------
# 3. GET /weekly_average: compute average daily trips per week
# ------------------------------------------------------------
@app.get("/weekly_average")
def weekly_average(
    mode: str = Query(..., regex="^(region|bbox)$"),
    region: Optional[str] = None,
    lat_min: Optional[float] = None,
    lat_max: Optional[float] = None,
    lon_min: Optional[float] = None,
    lon_max: Optional[float] = None,
):
    """
    Return the weekly average of trips per day.
    - mode="region": filter by a specific region.
    - mode="bbox": filter by bounding box (lat_min, lat_max, lon_min, lon_max).
    """
    logging.info(
        f"Received GET /weekly_average request | mode={mode} | "
        f"region={region} | lat_min={lat_min}, lat_max={lat_max}, "
        f"lon_min={lon_min}, lon_max={lon_max}"
    )

    conn = get_connection()
    cs = conn.cursor()

    try:
        if mode == "region":
            if not region:
                logging.warning("GET /weekly_average called without 'region' parameter")
                return {"error": "region parameter is required when mode=region"}

            sql = """
                SELECT
                  region,
                  DATE_TRUNC('week', departure_time) AS week,
                  COUNT(*) / 7 AS avg_trips_per_day
                FROM trips
                WHERE region = %s
                GROUP BY region, week
                ORDER BY week;
            """
            cs.execute(sql, (region,))
            rows = cs.fetchall()
            columns = [desc[0] for desc in cs.description]
            data = [dict(zip(columns, row)) for row in rows]
            logging.info(
                f"GET /weekly_average by region '{region}' returned {len(data)} rows."
            )
            return data

        elif mode == "bbox":
            if None in (lat_min, lat_max, lon_min, lon_max):
                logging.warning("GET /weekly_average called without complete bbox parameters")
                return {
                    "error": "lat_min, lat_max, lon_min, lon_max are required when mode=bbox"
                }

            sql = """
                SELECT
                  DATE_TRUNC('week', departure_time) AS week,
                  COUNT(*) / 7 AS avg_trips_per_day
                FROM trips
                WHERE origin_latitude  BETWEEN %s AND %s
                  AND origin_longitude BETWEEN %s AND %s
                GROUP BY week
                ORDER BY week;
            """
            cs.execute(sql, (lat_min, lat_max, lon_min, lon_max))
            rows = cs.fetchall()
            columns = [desc[0] for desc in cs.description]
            data = [dict(zip(columns, row)) for row in rows]
            logging.info(f"GET /weekly_average for bbox returned {len(data)} rows.")
            return data

        else:
            logging.error("GET /weekly_average called with invalid mode")
            return {"error": "Invalid mode"}

    except Exception as e:
        logging.error(f"Error querying /weekly_average: {e}")
        return {"error": str(e)}

    finally:
        cs.close()
        conn.close()


# ------------------------------------------------------------
# 4. POST /ingest: accept CSV upload and ingest into Snowflake
# ------------------------------------------------------------
@app.post("/ingest")
async def ingest_csv(file: UploadFile = File(...)):
    """
    Accept a CSV file upload, store it temporarily, and run the enrichment
    and load pipeline into Snowflake.
    Returns status and filename.
    """
    import traceback

    logging.info(f"Received POST /ingest for file: {file.filename}")

    # Save upload to a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        contents = await file.read()
        tmp.write(contents)
        tmp_path = tmp.name

    try:
        logging.info(f"Starting ingestion for file {file.filename} via API")
        enrich_and_save_to_snowflake(tmp_path)
        logging.info(f"Completed ingestion for file {file.filename} successfully")
        return {"status": "success", "filename": file.filename}

    except Exception as e:
        logging.error(f"Failed ingestion for file {file.filename}: {e}")
        logging.error(traceback.format_exc())
        return {"status": "error", "error": str(e), "filename": file.filename}
