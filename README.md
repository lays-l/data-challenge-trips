# Data Engineering Challenge – Trips Ingestion Pipeline

This project ingests, enriches, and analyzes trip data from `trips.csv`, fulfilling every requirement in the challenge while demonstrating scalability and production readiness.

## Requirements mapping

- **Automated ingestion** → `POST /ingest` triggers the Python pipeline in `api/main.py`  
- **Grouping by origin, destination and time of day** → `marts/mart_trip_by_city_and_tod.sql`  
- **Weekly average by region or bounding box** → `GET /weekly_average?mode=region|bbox` in `api/main.py`  
- **Real-time ingestion status (no polling)** → TODO. Planned to use Server-Sent Events
- **Scalability to 100 M records** → See **Scalability proof** below  
- **Use of SQL database** → Snowflake via `write_pandas`


## Scalability proof

- **Chunked processing**: read CSV with  
  ```python
  pd.read_csv(..., chunksize=50_000)
  ```  
- **Parallel upload**: dispatch concurrent `write_pandas` calls  
- **Synthetic benchmark**: use `notebook/generate_fake_trips.ipynb` to generate N records. Executed on google colab
- **Results**: Still under testing, since the ingestion results with a file containing 1 million rows were not satisfactory. There may be an issue with the use of the reverse_geocoder library, which needs further optimization.
<img width="1319" height="282" alt="image" src="https://github.com/user-attachments/assets/fb930835-8338-4942-83d7-6ee4f9b415ee" />


## Cloud deployment sketch (AWS example)

1. **S3** bucket `trips-uploads` stores incoming CSVs  
2. **Lambda** function triggers on S3 upload, invokes the ingestion API  
3. **ECS Fargate** hosts FastAPI service for `/ingest` and `/weekly_average`  
4. **DBT Cloud** schedules and runs transformation models  
5. **CloudWatch** captures logs and metrics; **SNS** sends alerts on failures

Also it's possible to use **Glue Jobs** and **Glue Tables** to use as Database instead of SnowFlake. This way the API could be used only for GET services, once that Glue Job will consume directly from **S3** buckets


## Bonus queries

Run extra SQL examples in `sql/queries.sql`:

```bash
snowsql -a $ACCOUNT -u $USER -f sql/queries.sql
```

- **Query 1**: fetch the last `datasource` for the two most common regions  
- **Query 2**: list regions containing `cheap_mobile`


## Getting started

1. Copy `.env.example` → `.env` and fill in:
   ```env
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_user
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_WAREHOUSE=your_warehouse
   SNOWFLAKE_DATABASE=your_database
   SNOWFLAKE_SCHEMA=PUBLIC
   SNOWFLAKE_ROLE=your_account_role
   ```
2. Build and run with Docker:
   ```bash
   docker-compose up --build
   ```
3. Local ingestion using `ingestion/data/trips.csv`  :
   ```bash
   docker-compose up ingest
   ```
4. Create grouped marts using DBT:
   ```bash
   docker-compose up dbt
   ```
5. Run API FastAPI (listens port 8000):
   ```bash
   docker-compose up api
   ```

## How to Use the API

The API will be available on `http://localhost:8000/docs#/`

### Get Weekly Average by **region**
```
GET /weekly_average?mode=region&region=RegionName
```
Example:
`http://localhost:8000/weekly_average?mode=region&region=Turin`

### Get Weekly Average by **bounding box**
```
GET /weekly_average?mode=bbox&lat_min=-24.0&lat_max=-23.0&lon_min=-47.0&lon_max=-46.0
```
Example:
`http://localhost:8000/weekly_average?mode=bbox&lat_min=-24.0&lat_max=-23.0&lon_min=-47.0&lon_max=-46.0`

The response will be a JSON with the weekly trip averages.

---

## Automated Data Ingestion (On-Demand)

You can upload a new CSV file directly to the pipeline using the following endpoint:

```
POST /ingest
Content-Type: multipart/form-data
Body: file=[arquivo CSV]
```

Example using `curl`:
```bash
curl -X POST "http://localhost:8000/ingest" -F "file=@dbt_project/seeds/trips.csv"
```

The response will be a JSON confirming the processing:
```json
{"status": "success", "filename": "trips.csv"}
```

The file will be enriched and the data will be automatically saved to Snowflake!

## API Reference

| Endpoint          | Method | Description                                           |
|-------------------|:------:|-------------------------------------------------------|
| `/ingest`         | POST   | Upload CSV to trigger enrichment and load to Snowflake|
| `/weekly_average` | GET    | Compute weekly averages by `region` or `bbox`         |



## Suggested Improvements
### Data Validation
Validate all input data before inserting it into the database to prevent corrupt or incomplete records.

### Table Name Parameterization
Allow the target table name to be set via parameter or environment variable for more flexible pipeline management.

### Snowflake Connection as a Class
Refactor the Snowflake connection into a dedicated class for easier reuse and future updates.

### Separate Raw and Enriched Data
Store raw data in one table and enriched data in another to preserve original records and simplify processing tracking.

### Automate Datamart Creation with DBT
Trigger DBT to build or update datamarts automatically after data ingestion.

### API Query by City or Country
Add API options to allow queries by city or country, not just by coordinates bounding box.
