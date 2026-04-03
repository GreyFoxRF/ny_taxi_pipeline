from pathlib import Path
from spark_session import create_spark_session
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType


# Абсолютный контроль путей
BASE_DIR = Path(__file__).resolve().parent.parent
CLEARED_DATA_PATH = str(BASE_DIR / 'data' / 'processed' / 'cleaned_yellow_tripdata')
ZONES_FLAGS = str(BASE_DIR / 'data' / 'raw' / 'taxi+_zone_lookup.csv')
REPORT_PATH = str(BASE_DIR / 'data' / 'report' / 'top_routes_mart')

def join_data(spark):
    print("--> Загрузка сырья...")

    zones_schema = StructType([
        StructField("LocationID", IntegerType(), True),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True)
    ])

    df_fact = spark.read.parquet(CLEARED_DATA_PATH)
    df_zone = spark.read.csv(ZONES_FLAGS, schema = zones_schema)

    print("--> Активация процесса объединения данных данных...")

    full_df =  df_fact.join(
        F.broadcast(df_zone).alias('pu'), on=(F.col('PULocationID') == F.col('pu.LocationID'))
        , how='left').join(
        F.broadcast(df_zone).alias('do'), on=(F.col('DOLocationID') == F.col('do.LocationID'))
        , how='left').select(
            df_fact.columns + [
                F.col('pu.Borough').alias('PU_borough'),
                F.col('pu.Zone').alias('PU_Zone'),
                F.col('pu.service_zone').alias('PU_service_zone'),
                F.col('do.Borough').alias('DO_borough'),
                F.col('do.Zone').alias('DO_Zone'),
                F.col('do.service_zone').alias('DO_service_zone')
            ]
        ).withColumn(
            'part_of_day',
            F.when((F.hour('tpep_pickup_datetime') >= 0) & (F.hour('tpep_pickup_datetime') < 6), '1_night').\
            when((F.hour('tpep_pickup_datetime') >= 6) & (F.hour('tpep_pickup_datetime') < 12), '2_morning').\
            when((F.hour('tpep_pickup_datetime') >= 12) & (F.hour('tpep_pickup_datetime') < 18), '3_day').\
            otherwise('4_evening')
        ).groupBy('part_of_day', 'PU_borough', 'DO_borough').agg(
            F.sum("total_amount").cast(DecimalType(15, 2)).alias('total_amount'),
            F.round(F.avg('total_amount'), 2).alias('avg_amount'),
            F.round(F.sum('trip_distance'), 2).alias('sum_trip_distance'),
            F.round(F.avg('trip_distance'), 2).alias('avg_trip_distance')
        ).withColumn(
            'order',
            F.row_number().over(Window.partitionBy('part_of_day').orderBy(F.col('total_amount').desc()))
            ).filter(F.col('order') <= 5).drop('order').\
                withColumn('amount_for_mile', (F.col('total_amount') / F.col('sum_trip_distance')).cast(DecimalType(15, 2))).\
                orderBy(F.col('part_of_day'), F.col('total_amount').desc())
    

    print("--> Выгрузка витрины данных для бизнеса...")

    full_df.coalesce(1).write.csv(
        REPORT_PATH,
        header=True,
        mode='overwrite',
        sep=','
    )
    
    print("--> Процесс завершен. Данные материализованы.")



if __name__ == "__main__":
    spark_session = create_spark_session()
    
    try:
        join_data(spark_session)
    finally:
        
        spark_session.stop()