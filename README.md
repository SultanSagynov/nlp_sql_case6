
# Text-to-SQL Chatbot API

API на базе **FastAPI** для анализа финансовых данных немецких компаний с использованием запросов на естественном языке.

---

##  Стек технологий

- **Бэкенд:** FastAPI, Uvicorn  
- **AI:** OpenAI (GPT-4o)  
- **База данных:** PostgreSQL  
- **Инструменты:** Docker, SQLAlchemy, Pandas  

---

##  Запуск проекта

### 1. Предварительные требования

- Установленный **Docker**
- Ключ **OpenAI API**
- Доступ к **PostgreSQL** с таблицей `top_12_german_companies` (можно скачать https://www.kaggle.com/datasets/willianoliveiragibin/top-12-german-companies)

---

### 2. Настройка окружения

Создайте файл `.env` в корне проекта и добавьте следующие переменные:

```env
# .env
API_AUTH_KEY=YourSecretApiTokenGoesHere
# По сути можно любую ллмку
OPENAI_API_KEY=sk-proj-YourOpenAIKeyGoesHere
DATABASE_URL=postgresql://user:password@host:port/database
```

---

### 3. Подготовка базы данных

Убедитесь, что в базе данных уже существует таблица `top_12_german_companies` и она заполнена данными из файла:

```
input_data/top_12_german_companies.csv
```

---

### 4. Сборка и запуск контейнера

Выполните в корневой директории:

```bash
docker-compose up --build
```

После запуска API будет доступен по адресу:  
 `http://localhost:8000`

Swagger UI (интерактивная документация):  
 `http://localhost:8000/docs`

---

##  Тестирование

### Локально через CLI

Для быстрого теста логики без запуска сервера:

```bash
# Установка зависимостей
pip install -r requirements.txt

# Пример запроса
python test-cli.py "какая суммарная выручка у всех компаний за 2023 год?"
```

---

### Через API (cURL)

Пример запроса к запущенному контейнеру:

```bash
curl -X 'POST' \
  'http://localhost:8000/chat' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer Pr0mpt0l0g0rPr0ct0l0g_A9!kLmN7#tQwYxZ4@h2sV' \
  -H 'Content-Type: application/json' \
  -d '{
  "query": "Сколько было доход у BMW в 2019 ?"
}'
```

---

##  Обратная связь

Если у вас есть предложения или замечания — не стесняйтесь открывать issue или прислать PR! 

---
