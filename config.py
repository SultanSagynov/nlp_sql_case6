import os
import openai
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv

# === Загрузка переменных окружения из .env файла ===
load_dotenv()

# === Настройка логирования ===
# Создаем единый логгер для всего приложения
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger("chatbot_app")

# === Безопасность API ===
API_AUTH_KEY = os.getenv("API_AUTH_KEY")
if not API_AUTH_KEY:
    logger.warning("API_AUTH_KEY не установлен. API будет работать без аутентификации.")

# === Настройка подключения к базе данных ===
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("Переменная окружения DATABASE_URL не установлена.")

# Создаем "движок" SQLAlchemy, который управляет пулом соединений к БД
db_engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # Проверяет соединение перед использованием
    pool_timeout=20,     # Время ожидания свободного соединения
    pool_recycle=1800    # Пересоздавать соединение каждые 30 минут для стабильности
)

# === Настройка OpenAI ===
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("Переменная окружения OPENAI_API_KEY не установлена.")

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")

# Создаем асинхронный клиент OpenAI, так как FastAPI работает асинхронно
openai_async_client = openai.AsyncOpenAI(
    api_key=OPENAI_API_KEY,
    base_url=OPENAI_API_BASE
)

logger.info("Конфигурация приложения успешно загружена.")