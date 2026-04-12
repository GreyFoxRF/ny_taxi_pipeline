#!/bin/bash

# Проверка
if [ -z "$1" ]; then
  echo "[ОШИБКА] Цель не задана. Укажи год запуска. Пример: ./run_pipeline.sh 2024"
  exit 1
fi

YEAR=$1

echo "=== ЗАПУСК МАССОВОЙ ЗАГРУЗКИ ЗА $YEAR ГОД ==="


for MONTH in {01..12}; do
  echo "-> Активация конвейера для $YEAR-$MONTH..."

  docker compose run --rm spark_app python src/main.py --year $YEAR --month $MONTH


  if [ $? -ne 0 ]; then
    echo "[КРИТИЧЕСКАЯ ОШИБКА] Сбой при обработке месяца $MONTH. Протокол массовой загрузки прерван."
    exit 1
  fi

  echo "-> Данные за $MONTH месяц успешно интегрированы."
  echo "----------------------------------------"
done

echo "=== МИССИЯ ВЫПОЛНЕНА. ИСТОРИЧЕСКИЕ ДАННЫЕ ЗАГРУЖЕНЫ ==="