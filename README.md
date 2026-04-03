# NY Taxi Data Pipeline & Profitability Data Mart

**Author:** Sergei Vashchuk | [LinkedIn](https://www.linkedin.com/in/sergei-vashchuk-33b390400/)

## Objective
An optimized end-to-end ETL pipeline built with PySpark to process, clean, and analyze raw New York City Yellow Taxi transactional data. The core objective is to transform noisy, high-volume data into a clean, business-ready Data Mart (CSV) that identifies the most profitable taxi routes based on the time of day.

## Tech Stack
* **Language:** Python
* **Big Data Processing:** PySpark (Spark SQL, DataFrame API) with Apache Arrow optimization
* **Orchestration:** Custom Python entry point (`main.py`)
* **Exploratory Data Analysis (EDA):** DuckDB, Jupyter Notebook
* **Data Formats:** Parquet (intermediate storage), CSV (final business Data Mart)

## Architecture & Engineering Decisions

1. **Orchestration (`main.py`):**
   * Designed a single entry point for the entire pipeline. The process runs end-to-end without manual intervention.
   * `SparkSession` is initialized exactly once and passed as a resource through the pipeline modules, preventing memory leaks and overhead from multiple JVM startups.
2. **Extraction (`download_data.py`):**
   * Automated batch downloading of raw Parquet files and dimensional CSV lookup tables directly from source URLs.
3. **Data Cleaning (`clear_data.py`):**
   * Enforced strict data quality rules: filtered out anomalies such as zero or negative trip distances, empty fares, and transactions falling outside the target analytical window (January 2024).
4. **Enrichment & Advanced Analytics (`enrich_data.py`):**
   * **Performance Optimization:** Implemented a **Broadcast Hash Join** (`F.broadcast`) to merge the massive fact table with a dimensional lookup table (Zone IDs). This architecture decision completely eliminated expensive network shuffles.
   * **Feature Engineering:** Segmented raw timestamps into discrete time-of-day buckets (Morning, Day, Evening, Night). Created a derived business metric, `amount_for_mile`, to evaluate the true economic efficiency of each route.
   * **Window Functions:** Applied `Window.partitionBy().orderBy()` to accurately rank and extract the Top-5 most profitable routes strictly within each time-of-day segment.
5. **Data Materialization:**
   * Forced the distributed Spark DataFrame into a single CSV file using `coalesce(1)` to accommodate downstream analysts using Excel.
   * Explicitly cast financial metrics to `DecimalType(15, 2)` to guarantee precision and prevent exponential notation artifacts in the final report.

## Quick Start

```bash
# Clone the repository
git clone <your-repo-link>
cd ny_taxi_pipeline

# Install dependencies
pip install -r requirements.txt

# Execute the full pipeline (Download -> Clean -> Enrich -> Export)
python src/main.py
```

## Project Structure

ny_taxi_pipeline/
├── data/
│   ├── raw/                  # Downloaded raw datasets
│   ├── processed/            # Cleaned intermediate data (Parquet)
│   └── report/               # Final Data Mart (CSV)
├── notebooks/                # EDA with DuckDB and PySpark prototyping
├── src/
│   ├── main.py               # Orchestrator / Entry point
│   ├── clear_data.py         # Anomaly filtration module
│   ├── download_data.py      # Data extraction module
│   ├── enrich_data.py        # Core ETL, aggregations, and Window functions
│   └── spark_session.py      # Spark and Arrow configuration
├── requirements.txt          # Environment dependencies
└── README.md                 # Project documentation
