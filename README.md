# NY Taxi Data Pipeline & Profitability Data Mart

**Author:** Sergei (Fox) Vashchuk | [LinkedIn](https://www.linkedin.com/in/sergei-vashchuk-33b390400/)

## Objective
An optimized, idempotent end-to-end ETL pipeline built with PySpark and Docker. It processes, cleans, and analyzes raw New York City Yellow Taxi transactional data. The core objective is to transform noisy, high-volume data into a clean, business-ready Data Mart stored in PostgreSQL, which identifies the most profitable taxi routes based on the time of day.

## Tech Stack
* **Language:** Python 3.12
* **Big Data Processing:** PySpark (Spark SQL, DataFrame API) with Apache Arrow optimization
* **Database:** PostgreSQL 15 (via JDBC & psycopg2)
* **Orchestration:** Custom Python entry point (`main.py`) & Bash automation (`run_pipeline.sh`)
* **Infrastructure & Containerization:** Docker & Docker Compose

## Architecture & Engineering Decisions

1. **Security & Configuration:**
   * Strict separation of configuration from code. Database credentials and environment variables are securely managed via a `.env` file, preventing hardcoded secrets in the repository.
2. **Dynamic Orchestration & Pre-flight Checks (`main.py`, `check_url.py`):**
   * Pipeline is fully parameterized via CLI arguments (`--year`, `--month`), allowing dynamic processing of any historical period.
   * Built-in HTTP pre-flight scanner validates the existence of source data on the NYC Taxi servers before allocating Spark resources, preventing silent failures.
3. **Extraction & Cleaning (`download_data.py`, `clear_data.py`):**
   * Automated batch downloading of Parquet files and dimensional lookup tables.
   * Strict data quality enforcement: filtering out anomalies (negative distances, empty fares) and strictly bounding data to the requested execution month.
4. **Enrichment & Advanced Analytics (`enrich_data.py`):**
   * **Performance Optimization:** Implemented **Broadcast Hash Join** (`F.broadcast`) to merge massive fact tables with dimensional lookup zones, eliminating expensive network shuffles.
   * **Feature Engineering:** Segmented timestamps into discrete time-of-day buckets (Morning, Day, Evening, Night) and derived the `amount_for_mile` metric to evaluate route economic efficiency.
   * **Window Functions:** Applied `Window.partitionBy().orderBy()` to accurately rank the Top-5 most profitable routes strictly within each time-of-day segment.
5. **Idempotent Data Materialization (`upload_data.py`):**
   * Designed a robust, idempotent loading mechanism to PostgreSQL. 
   * A surgical cleanup operation using `psycopg2` deletes existing records for the target `(year, month)` partition before Spark appends the new data via JDBC. This guarantees zero duplication even with multiple pipeline reruns for the same period.
6. **Automated Backfill (`run_pipeline.sh`):**
   * A dedicated bash orchestrator allows for seamless historical data backfills. It iteratively triggers isolated Spark containers for an entire year, automatically handling failures and preventing memory leaks.

## Quick Start

### 1. Environment Setup
Before building the infrastructure, you must configure your environment variables. Create a `.env` file in the root directory:

```env
# Database initialization
POSTGRES_USER=your_db_user
POSTGRES_PASSWORD=your_db_password
POSTGRES_DB=ny_taxi

# Spark connection parameters
DB_HOST=db
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_NAME=ny_taxi
2. Build and Initialize Infrastructure
Bash
# Clone the repository
git clone [https://github.com/GreyFoxRF/ny_taxi_pipeline.git](https://github.com/GreyFoxRF/ny_taxi_pipeline.git)
cd ny_taxi_pipeline

# Start the PostgreSQL database
docker compose up -d db

# Build the Spark ETL image
docker compose build spark_app
3. Run the Pipeline
Option A: Single Month Processing
Trigger the pipeline for a specific year and month:

Bash
# Example: Process data for February 2024
docker compose run --rm spark_app python src/main.py --year 2024 --month 02
Option B: Automated Historical Backfill (Full Year)
Run the batch orchestrator to process an entire year sequentially:

Bash
# Make the script executable (first time only)
chmod +x run_pipeline.sh

# Example: Process all available data for 2024
./run_pipeline.sh 2024
Project Structure
Plaintext
ny_taxi_pipeline/
├── data/
│   ├── raw/                  # Downloaded raw datasets
│   └── processed/            # Cleaned intermediate data (Parquet)
├── src/
│   ├── main.py               # Orchestrator / CLI Entry point
│   ├── check_url.py          # Pre-flight HTTP validation
│   ├── clear_data.py         # Anomaly filtration module
│   ├── download_data.py      # Data extraction module
│   ├── enrich_data.py        # Core ETL, aggregations, and Window functions
│   ├── upload_data.py        # PostgreSQL idempotent loader
│   ├── spark_session.py      # Spark and Arrow configuration
│   └── logger.py             # Centralized logging configuration
├── run_pipeline.sh           # Bash orchestrator for historical backfill
├── docker-compose.yml        # Multi-container infrastructure setup
├── Dockerfile                # Spark & Python environment image
├── requirements.txt          # Environment dependencies
├── .env.example              # (Optional) Template for environment variables
└── README.md                 # Project documentation
```