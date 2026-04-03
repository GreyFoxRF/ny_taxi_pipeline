import pyspark.sql.functions as F
from pathlib import Path
from logger import setup_logger
logger = setup_logger()

# Абсолютный контроль путей
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_PATH = str(BASE_DIR / 'data' / 'raw' / 'yellow_tripdata_2024-01.parquet')
PROCESSED_DATA_PATH = str(BASE_DIR / 'data' / 'processed' / 'cleaned_yellow_tripdata')

def clean_data(spark):
    logger.info("--> Загрузка сырья...")
    df = spark.read.parquet(RAW_DATA_PATH)
    
    initial_count = df.count()
    logger.info(f"--> Обнаружено строк до фильтрации: {initial_count}")

    logger.info("--> Активация протокола очистки данных...")
    
    cleaned_df = df.filter(
        (F.col("trip_distance") > 0) &
        (F.col("trip_distance") < 150) &
        (F.col("total_amount") > 0) &
        (F.col("tpep_pickup_datetime") >= "2024-01-01 00:00:00") &
        (F.col("tpep_pickup_datetime") < "2024-02-01 00:00:00")
    )

    final_count = cleaned_df.count()
    logger.info(f"--> Строк после фильтрации: {final_count}")
    logger.info(f"--> Уничтожено аномалий: {initial_count - final_count}")

    logger.info("--> Запись чистой витрины на диск...")

    cleaned_df.write.mode("overwrite").parquet(PROCESSED_DATA_PATH)
    logger.info(f"!!! Операция завершена. Данные сохранены в {PROCESSED_DATA_PATH}")