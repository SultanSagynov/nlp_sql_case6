# Используем официальный легковесный образ Python 3.10
FROM python:3.10-slim

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Устанавливаем системные зависимости, необходимые для psycopg2 (драйвер PostgreSQL)
# --no-install-recommends - не устанавливать необязательные пакеты
# rm -rf /var/lib/apt/lists/* - очищаем кеш после установки, чтобы уменьшить размер образа
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Копируем файл с зависимостями в контейнер
COPY requirements.txt .

# Устанавливаем Python-зависимости
# --no-cache-dir - не сохранять кеш, чтобы уменьшить размер образа
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь остальной код приложения в рабочую директорию /app
COPY . .

# Устанавливаем переменную окружения, чтобы Python выводил логи сразу, без буферизации
ENV PYTHONUNBUFFERED=1

# Сообщаем Docker, что приложение будет слушать порт 8000
EXPOSE 8000