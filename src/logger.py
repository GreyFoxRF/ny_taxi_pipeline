import logging
from logging.handlers import RotatingFileHandler
import sys
from pathlib import Path

def setup_logger(name="NYTaxiPipeline"):
    logger = logging.getLogger(name)
    
    # Защита от дублирования записей, если логгер вызывается несколько раз
    if logger.hasHandlers():
        return logger
        
    logger.setLevel(logging.INFO)

    # Строгий формат: [Время] - [Уровень] - [Модуль] - Сообщение
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(module)s] - %(message)s', 
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 1. Вывод логов в терминал
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 2. Запись логов в файл (сохранение истории)
    log_dir = Path(__file__).resolve().parent.parent / "logs"
    log_dir.mkdir(exist_ok=True) # Автоматически создаст папку logs, если её нет
    
    # 5 * 1 048 576 backupCount=5
    file_handler = RotatingFileHandler(
        filename=log_dir / "pipeline.log", 
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding='utf-8'
        )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger