import os
import openai
import logging
import httpx
from sqlalchemy import create_engine
from dotenv import load_dotenv
from typing import List, Dict

# === Загрузка .env ===
load_dotenv()

# === Логирование ===
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger("chatbot_app")

# === Конфигурация API и БД ===
API_AUTH_KEY = os.getenv("API_AUTH_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL не установлена.")
db_engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# === Конфигурация LLM Провайдеров ===
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai").lower()

# Кастомный LLM
CUSTOM_LLM_API_BASE = os.getenv("CUSTOM_LLM_API_BASE")
CUSTOM_LLM_MODEL = os.getenv("CUSTOM_LLM_MODEL")
CUSTOM_LLM_API_KEY = os.getenv("CUSTOM_LLM_API_KEY")

# OpenAI (как основной или запасной)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")
openai_async_client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)

# Создаем единый асинхронный HTTP клиент для всех запросов
async_http_client = httpx.AsyncClient(timeout=120.0)

async def get_llm_completion(messages: List[Dict[str, str]], temperature: float) -> str:
    """
    Универсальная функция для вызова LLM.
    Пытается использовать основного провайдера, при ошибке переключается на OpenAI.
    """
    use_custom_llm = LLM_PROVIDER == 'custom' and CUSTOM_LLM_API_BASE and CUSTOM_LLM_MODEL

    if use_custom_llm:
        try:
            logger.info(f"Вызов кастомного LLM: {CUSTOM_LLM_MODEL}")
            headers = {"Content-Type": "application/json"}
            if CUSTOM_LLM_API_KEY:
                headers["Authorization"] = f"Bearer {CUSTOM_LLM_API_KEY}"
            
            payload = {
                "messages": messages,
                "model": CUSTOM_LLM_MODEL,
                "temperature": temperature,
                "max_completion_tokens": 1024, 
                "stream": False
            }
            
            response = await async_http_client.post(
                f"{CUSTOM_LLM_API_BASE.rstrip('/')}/chat/completions",
                json=payload,
                headers=headers
            )
            response.raise_for_status() 
            
            data = response.json()
            return data["choices"][0]["message"]["content"]
        
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            logger.warning(f"Ошибка при вызове кастомного LLM: {e}. Переключение на OpenAI.")
            # При ошибке автоматически переходим к запасному варианту

    try:
        logger.info(f"Вызов OpenAI LLM: {OPENAI_MODEL}")
        if not OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY не установлен для запасного варианта.")
            
        response = await openai_async_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=messages,
            temperature=temperature
        )
        return response.choices[0].message.content
    except Exception as e:
        logger.error(f"Критическая ошибка: оба LLM провайдера недоступны. Ошибка OpenAI: {e}")
        raise IOError("Сервис генерации текста временно недоступен.")