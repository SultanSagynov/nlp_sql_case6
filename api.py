from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from typing import Optional

# --- Импорт конфигурации и основной логики ---
from config import logger, API_AUTH_KEY
from pipeline import run_companies_pipeline

# --- Инициализация FastAPI приложения ---
app = FastAPI(
    title="Промптологи - Shai Pro Case 6",
    description="Демо API для анализа финансовых данных (12_german_c).",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"], 
)

# --- Событие при старте приложения ---
@app.on_event("startup")
async def startup_event():
    logger.info("API сервер успешно запущен.")

# --- Безопасность: схема и функция для проверки API ключа ---
auth_scheme = HTTPBearer()

def verify_api_key(credentials: Optional[HTTPAuthorizationCredentials] = Depends(auth_scheme)):
    """
    Проверяет, что в заголовке Authorization передан правильный Bearer токен.
    """
    if not API_AUTH_KEY:
        logger.warning("API_AUTH_KEY не установлен; аутентификация пропускается.")
        return
    
    if credentials is None or credentials.credentials != API_AUTH_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Неверный или отсутствующий API ключ"
        )

# --- Модели данных Pydantic для валидации запросов и ответов ---
class ChatRequest(BaseModel):
    """Модель для входящего запроса."""
    query: str = Field(..., min_length=1, description="Текстовый запрос пользователя")

class ChatResponse(BaseModel):
    """Модель для исходящего ответа."""
    answer: str

# --- HTTP Эндпоинты ---

@app.get("/health", summary="Проверка состояния сервиса")
def health_check():
    """
    Простой эндпоинт для проверки, что API сервис запущен и отвечает на запросы.
    Используется системами мониторинга.
    """
    return {"status": "ok"}

@app.post("/chat", 
          response_model=ChatResponse, 
          summary="Отправить запрос чат-боту",
          dependencies=[Depends(verify_api_key)])
async def http_chat_endpoint(request: ChatRequest):
    """
    Основной эндпоинт для взаимодействия с ботом.
    Принимает вопрос пользователя, передает его в пайплайн и возвращает ответ.
    """
    try:
        logger.info(f"Получен запрос: '{request.query}'")
        # Вызываем основную логику из pipeline.py
        answer = await run_companies_pipeline(request.query)
        return ChatResponse(answer=answer)
    except Exception as e:
        logger.exception("Критическая ошибка при обработке запроса в эндпоинте /chat.")
        # Возвращаем общую ошибку, чтобы не раскрывать детали реализации
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                            detail="Внутренняя ошибка сервера.")