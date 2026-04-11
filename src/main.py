from download_data import download_data
from clear_data import clean_data
from spark_session import create_spark_session
from enrich_data import enrich_data
from pathlib import Path
from logger import setup_logger
from check_url import check_url
import argparse
from upload_data import upload_data

parser = argparse.ArgumentParser()
parser.add_argument('--year', type=str, required=True, help='Год запуска пайплайна (например, 2024)')
parser.add_argument('--month', type=str, required=True, help='Месяц запуска пайплайна (например, 01)')

logger = setup_logger()
args = parser.parse_args()

if len(args.month) == 1:
    args.month = f'0{args.month}'

logger.info(f"Запуск для {args.year}-{args.month}")

url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{args.year}-{args.month}.parquet'
URLS = [
    url,
    'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
]
BASE_DIR = Path(__file__).resolve().parent.parent
FILE_PATH = BASE_DIR / 'data' / 'raw'

def run(spark):
    if not check_url(URLS):
        exit()

    logger.info("=== ЭТАП 2: ДОБЫЧА СЫРЬЯ ===")
    download_data(URLS, FILE_PATH)

    logger.info("=== ЭТАП 3: ОЧИСТКА ДАННЫХ ===")
    clean_data(spark, args.year, args.month)

    logger.info("=== STAGE 4: CREATING THE BUSINESS SHOWCASE ===")
    df = enrich_data(spark, args.year, args.month)

    logger.info("=== STAGE 5: Uploading the Business Data Mart to the Server ===")
    upload_data(spark, df, args.year, args.month)

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