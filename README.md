# NY Taxi Data Pipeline & Profitability Data Mart

**Author:** Sergei (Fox) Vashchuk | [LinkedIn](https://www.linkedin.com/in/sergei-vashchuk-33b390400/)

## ⛩ Mission
An automated, idempotent, end-to-end ETL pipeline built with PySpark, Docker, and Apache Airflow. It processes, cleans, and analyzes raw New York City Yellow Taxi transactional data. The core objective is to transform noisy, high-volume data into a clean, business-ready Data Mart stored in PostgreSQL, which identifies the Top-5 most profitable taxi routes based on the time of day.

## 🛠 Tech Stack
* **Orchestration:** Apache Airflow 2.8 (DAGs, Catchup, Backfilling)
* **Big Data Engine:** Apache Spark (PySpark) with Apache Arrow optimization
* **Database:** PostgreSQL 15 (via JDBC & psycopg2)
* **Infrastructure:** Docker & Docker Compose (Docker-in-Docker architecture)
* **Language:** Python 3.12

## 🏗 Architecture & Engineering Decisions

1. **Industrial Orchestration (Airflow):**
   * Migrated from manual bash-scripts to fully automated Airflow DAGs.
   * Leveraged Airflow's `catchup=True` mechanism for automatic historical backfilling.
   * Utilized Docker-in-Docker (DinD) approach: Airflow container dynamically triggers isolated Spark containers for each execution month.
2. **Resource Management & OOM Protection:**
   * Hardcoded memory limits (2GB driver/executor) in `spark_session.py` to prevent Out-Of-Memory (OOM) kills during heavy Parquet processing.
   * Disabled host volume mounting for source code in production to ensure immutable container execution.
3. **Extraction & Cleaning:**
   * Pre-flight HTTP validation (`check_url.py`) prevents resource allocation if source data is missing.
   * Automated downloading and strict data quality enforcement (filtering negative distances, zero fares, and bounding records to the execution month).
4. **Enrichment & Advanced Analytics:**
   * **Broadcast Hash Join:** Used `F.broadcast` to merge massive fact tables with dimensional lookup zones, eliminating expensive network shuffles.
   * **Feature Engineering:** Segmented timestamps into discrete time-of-day buckets (Night, Morning, Day, Evening) and derived the `amount_for_mile` metric.
   * **Window Functions:** Applied `Window.partitionBy().orderBy()` to rank the Top-5 most profitable routes within each temporal segment.
5. **Idempotent Data Materialization:**
   * Designed a robust, idempotent loading mechanism to PostgreSQL. 
   * A surgical cleanup operation deletes existing records for the target `(year, month)` partition before Spark appends the new data. This guarantees zero duplication even during pipeline reruns.

## 🚀 Quick Start

### 1. Environment Setup
Create a `.env` file in the root directory to securely manage credentials:

```env
# Database initialization
POSTGRES_USER=fox
POSTGRES_PASSWORD=password123
POSTGRES_DB=ny_taxi

# Spark connection parameters
DB_HOST=db
DB_USER=fox
DB_PASSWORD=password123
DB_NAME=ny_taxi
2. Build and Initialize Infrastructure
Deploy the Command Center (Airflow) and the Vault (PostgreSQL):

Bash
# Clone the repository
git clone [https://github.com/GreyFoxRF/ny_taxi_pipeline.git](https://github.com/GreyFoxRF/ny_taxi_pipeline.git)
cd ny_taxi_pipeline

# Build the Spark image and start the infrastructure
docker compose up -d --build
3. Activate the Pipeline
Open the Airflow UI at http://localhost:8080.

Login with default credentials (admin / admin).

Locate the ny_taxi_historical_backfill DAG.

Toggle the DAG from Paused to Active.

Airflow will automatically calculate the missing months since January 2024 and sequentially spawn Spark containers to backfill the database.

📁 Project Structure
Plaintext
ny_taxi_pipeline/
├── dags/
│   └── ny_taxi_backfill_dag.py # Airflow DAG for monthly orchestration
├── src/
│   ├── main.py               # Application entry point
│   ├── check_url.py          # Pre-flight HTTP validation
│   ├── clear_data.py         # Anomaly filtration module
│   ├── download_data.py      # Data extraction module
│   ├── enrich_data.py        # Core ETL, aggregations, and Window functions
│   ├── upload_data.py        # PostgreSQL idempotent loader
│   ├── spark_session.py      # Spark configuration and OOM protection
│   └── logger.py             # Centralized logging configuration
├── docker-compose.yml        # Multi-container infrastructure (Airflow, Spark, Postgres)
├── Dockerfile                # Spark & Python environment image
├── requirements.txt          # Environment dependencies
└── README.md                 # Project documentation
```