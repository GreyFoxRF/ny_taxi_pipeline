# 1. Базовая матрица: Python 3.12 на базе легковесного Linux (Debian)
FROM python:3.12-slim

# 2. Установка Java (необходимо для работы ядра PySpark)
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 3. Фиксация рабочей директории внутри контейнера
WORKDIR /app

# 4. Копируем файл зависимостей с хоста в контейнер
COPY requirements.txt /app/

# 5. Устанавливаем Python-библиотеки (pandas, pyspark и т.д.)
RUN pip install --no-cache-dir -r requirements.txt

# 6. Переносим весь исходный код в контейнер
COPY src/ /app/src/

# 7. Финальный приказ: при запуске контейнера активировать Оркестратор
CMD ["python", "src/main.py"]