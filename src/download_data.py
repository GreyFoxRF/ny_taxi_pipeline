import requests
from pathlib import Path
from logger import setup_logger

logger = setup_logger()

URLS = [
    'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet',
    'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
]


FILE_PATH = Path('data/raw/')

def download_data(urls: list, save_path: Path):

    for url in urls:          
        logger.info(f"--> Starting to download from {url}...")
        
        
        response = requests.get(url, stream=True) 
        response.raise_for_status() 
        
        file_name = url.split('/')[-1]
        full_path = save_path / file_name

        with open(full_path, 'wb') as f:
            # iter_content скачивает файл блоками по 8 КБ
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        logger.info(f"Downloading is completed to: {save_path}")