import os
import sys
from pyspark.sql import SparkSession

# Жесткая фиксация среды выполнения для Spark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Указываем путь к нашей новой папке
os.environ['HADOOP_HOME'] = r'C:\hadoop'
# Добавляем её в системный путь, чтобы Spark нашел файлы
os.environ['PATH'] = os.environ['HADOOP_HOME'] + r'\bin;' + os.environ.get('PATH', '')

def create_spark_session():
    print("--> Инициализация ядра PySpark...")
    return SparkSession.builder \
        .appName("NYTaxiCleaner") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()