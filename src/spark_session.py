import os
import sys
from pyspark.sql import SparkSession
from logger import setup_logger

logger = setup_logger()

# Жесткая фиксация среды выполнения для Spark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def create_spark_session():
    logger.info("--> Инициализация ядра PySpark...")
    return SparkSession.builder \
        .appName("NYTaxiCleaner") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
