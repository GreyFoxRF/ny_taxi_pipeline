from download_data import download_data
from clear_data import clean_data
from spark_session import create_spark_session
from enrich_data import join_data
from pathlib import Path

URLS = [
    'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet',
    'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
]
BASE_DIR = Path(__file__).resolve().parent.parent
FILE_PATH = BASE_DIR / 'data' / 'raw'

def run(spark):
    download_data(URLS, FILE_PATH)

    clean_data(spark)

    join_data(spark)

if __name__ == "__main__":
    spark = create_spark_session()
    
    try:
        run(spark)
    finally:
        
        spark.stop()