import requests
from logger import setup_logger

logger = setup_logger()

def check_url(urls: list):
    for url in urls:
        logger.info(f"--> Проверка ответа сервера для {url}...")
        response = requests.head(url, allow_redirects=True, timeout=5)
        if response.status_code != 200:
            logger.error(f"[КРИТИЧЕСКАЯ ОШИБКА]: ошибка URL. Код сервера={response.status_code}")
            return False
    return True