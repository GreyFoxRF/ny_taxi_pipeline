from download_data import download_data
from clear_data import clean_data
from spark_session import create_spark_session
from enrich_data import join_data
from pathlib import Path
from logger import setup_logger

logger = setup_logger()

URLS = [
    'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet',
    'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
]
BASE_DIR = Path(__file__).resolve().parent.parent
FILE_PATH = BASE_DIR / 'data' / 'raw'

def run(spark):
    logger.info("=== ЭТАП 2: ДОБЫЧА СЫРЬЯ ===")
    download_data(URLS, FILE_PATH)

    logger.info("=== ЭТАП 3: ОЧИСТКА ДАННЫХ ===")
    clean_data(spark)

    logger.info("=== ЭТАП 4: ФОРМИРОВАНИЕ БИЗНЕС-ВИТРИНЫ ===")
    join_data(spark)

if __name__ == "__main__":
    logger.info("[ORCHESTRATOR] Инициализация генерального протокола...")
    
    logger.info("=== ЭТАП 1: ЗАПУСК ВЫЧИСЛИТЕЛЬНОГО ЯДРА ===")
    spark = create_spark_session()
    
    try:
        run(spark)
        logger.info("[ORCHESTRATOR] Миссия выполнена. Пайплайн успешно закрыт.")

    except Exception as e:
        logger.error(f"[КРИТИЧЕСКАЯ ОШИБКА]: {e}")
    
    finally:
        spark.stop()
        logger.info("[ORCHESTRATOR] Ядро Spark деактивировано. Ресурсы освобождены.")