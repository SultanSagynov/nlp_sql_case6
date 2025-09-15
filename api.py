from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from enum import Enum
from typing import Optional

# --- Импорт конфигурации и пайплайна ---
from config import logger, API_AUTH_KEY
from pipeline import run_companies_pipeline

# --- Enum для выбора модуля в API ---
# На данный момент у нас один модуль, но это позволяет легко добавлять новые в будущем
class BotModule(str, Enum):
    companies = "companies"

# --- Диспетчер для вызова нужного пайплайна ---
# Связывает имя модуля из Enum с функцией пайплайна
PIPELINE_DISPATCHER = {
    BotModule.companies: run_companies_pipeline,
}

# --- Инициализация FastAPI ---
app = FastAPI(
    title="Promptологи",
    description="Демо API для запросов финансовых данных по ведущим немецким компаниям.",
    version="Demo 1.0.0"
)

# --- Настройка CORS (позволяет запросы с других доменов) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    logger.info("API сервер успешно запущен.")

# --- Безопасность: проверка API ключа ---
auth_scheme = HTTPBearer()
def verify_api_key(credentials: Optional[HTTPAuthorizationCredentials] = Depends(auth_scheme)):
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
    query: str = Field(..., min_length=1, description="Текстовый запрос пользователя")
    module: BotModule = Field(..., description="Модуль для обработки запроса")

class ChatResponse(BaseModel):
    answer: str
    module: str

# --- HTTP эндпоинты ---
@app.get("/health", summary="Проверка состояния сервиса")
def health_check():
    """Проверяет, что API сервис запущен и работает."""
    return {"status": "ok"}

@app.post("/chat", response_model=ChatResponse, summary="Отправить запрос чат-боту")
async def http_chat_endpoint(request: ChatRequest, _=Depends(verify_api_key)):
    """Основной эндпоинт для взаимодействия с ботом."""
    pipeline_func = PIPELINE_DISPATCHER.get(request.module)
    if not pipeline_func:
        raise HTTPException(status_code=404, detail=f"Модуль '{request.module.value}' не найден.")
    
    try:
        logger.info(f"HTTP запрос для модуля: {request.module.value}")
        answer = await pipeline_func(request.query)
        return ChatResponse(answer=answer, module=request.module.value)
    except Exception as e:
        logger.exception(f"Ошибка в модуле {request.module.value} при обработке запроса.")
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {e}")