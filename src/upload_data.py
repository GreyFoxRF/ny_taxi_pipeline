import os
from logger import setup_logger
import psycopg2
    
logger = setup_logger()

def upload_data(spark, df, year, month):
    
    db_host = os.getenv('DB_HOST')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')

    logger.info(f"--> Зачистка старых данных за {year}-{month}...")
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=5432
        )
        cur = conn.cursor()
        
        # Удаляем старые партиции, если они были
        cur.execute(f"DELETE FROM top_routes_mart WHERE year = '{year}' AND month = '{month}'")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"[КРИТИЧЕСКАЯ ОШИБКА] Сбой при зачистке базы: {e}")
        # Если не смогли удалить старое - лучше остановить конвейер, чтобы не задублировать данные
        import sys
        sys.exit(1)




    logger.info(f"--> Запись на сервер...")
    db_connector = f'jdbc:postgresql://{db_host}:5432/{db_name}'


    logger.info("--> Uploading...")
    df.coalesce(1).write.jdbc(
        url=db_connector,
        table='top_routes_mart',
        mode='append',
        properties={
            'user': db_user,
            'password': db_password,
            'driver': 'org.postgresql.Driver'
        }
    )