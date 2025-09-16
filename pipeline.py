import json
import pandas as pd
import re
import sqlparse
from sqlalchemy import text
from typing import Dict
from sqlparse.exceptions import SQLParseError

# --- Импорт общих ресурсов и утилит ---
from config import db_engine, logger, get_llm_completion
from utils import format_numbers_in_df

# --- Конфигурация, специфичная для этого пайплайна ---
# Указываем путь к файлам с метаданными
BOT_BASE_DIR = "metadata_output"
BOT_CONFIG = {
    "table_name_db": "top_12_german_companies",
    "schema_path": f"{BOT_BASE_DIR}/top_12_german_companies_schema.json",
    "catalog_path": f"{BOT_BASE_DIR}/top_12_german_companies_catalog.json",
}

def load_json(path: str) -> Dict:
    """Загружает JSON файл с обработкой ошибок."""
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Файл не найден: {path}")
        raise ValueError(f"Конфигурационный файл не найден: {path}")
    except json.JSONDecodeError:
        logger.error(f"Невалидный JSON файл: {path}")
        raise ValueError(f"Ошибка в формате конфигурационного файла: {path}")

async def generate_sql(user_query: str, schema: Dict, catalog: Dict, table_name: str) -> Dict:
    """
    Генерирует SQL-запрос на основе запроса пользователя, используя LLM
    с тщательно подобранными примерами (few-shot prompting).
    """

    # Блок с примерами для обучения модели "на лету"
    few_shot_examples = """
# Пример 1: Простой поиск по одному показателю и одной компании
Запрос: какая выручка у Volkswagen за 2023 год?
Ответ:
{
  "sql": "SELECT \\"Company\\", \\"Period\\", \\"Revenue\\" FROM dmart.top_12_german_companies WHERE \\"Company\\" = 'Volkswagen AG' AND \\"Period\\" LIKE '%2023' ORDER BY \\"Period\\"",
  "clarified_prompt": "Какая была выручка у компании Volkswagen AG за все периоды в 2023 году?",
  "metrics": ["Revenue"],
  "groups": ["Volkswagen AG"],
  "years": [2023],
  "units": []
}

# Пример 2: Поиск нескольких показателей для одной компании
Запрос: покажи активы и обязательства для BMW в 2022
Ответ:
{
  "sql": "SELECT \\"Period\\", \\"Assets\\", \\"Liabilities\\" FROM dmart.top_12_german_companies WHERE \\"Company\\" = 'BMW AG' AND \\"Period\\" LIKE '%2022' ORDER BY \\"Period\\"",
  "clarified_prompt": "Какие были активы и обязательства у компании BMW AG в 2022 году?",
  "metrics": ["Assets", "Liabilities"],
  "groups": ["BMW AG"],
  "years": [2022],
  "units": []
}

# Пример 3: Агрегация (сумма) по всем компаниям за определенный период
Запрос: какая была суммарная чистая прибыль всех компаний в 2023 году?
Ответ:
{
  "sql": "SELECT SUM(\\"Net Income\\") AS \\"Total Net Income\\" FROM dmart.top_12_german_companies WHERE \\"Period\\" LIKE '%2023'",
  "clarified_prompt": "Какая была суммарная чистая прибыль всех компаний за 2023 год?",
  "metrics": ["Net Income"],
  "groups": [],
  "years": [2023],
  "units": []
}

# Пример 4: Агрегация с группировкой
Запрос: посчитай общую выручку для каждой компании за все время
Ответ:
{
  "sql": "SELECT \\"Company\\", SUM(\\"Revenue\\") AS \\"Total Revenue\\" FROM dmart.top_12_german_companies GROUP BY \\"Company\\" ORDER BY \\"Total Revenue\\" DESC",
  "clarified_prompt": "Какая общая выручка у каждой компании за весь доступный период?",
  "metrics": ["Revenue"],
  "groups": [],
  "years": [],
  "units": []
}

# Пример 5: Запрос с использованием сложного названия колонки из схемы
Запрос: какой ROE у компании SAP?
Ответ:
{
  "sql": "SELECT \\"Period\\", \\"ROE (%)\\" FROM dmart.top_12_german_companies WHERE \\"Company\\" = 'SAP SE' ORDER BY \\"Period\\" DESC",
  "clarified_prompt": "Какой показатель ROE (%) был у компании SAP SE за все периоды?",
  "metrics": ["ROE (%)"],
  "groups": ["SAP SE"],
  "years": [],
  "units": ["%"]
}

# Пример 6: Обработка неоднозначного запроса о годовом итоге
Запрос: какая была выручка у Даймлер в 2019?
Ответ:
{
  "sql": "SELECT SUM(\\"Revenue\\") AS \\"Total Annual Revenue\\" FROM dmart.top_12_german_companies WHERE \\"Company\\" = 'Daimler AG' AND \\"Period\\" LIKE '%2019'",
  "clarified_prompt": "Какая была суммарная годовая выручка у компании Daimler AG за 2019 год?",
  "metrics": ["Revenue"],
  "groups": ["Daimler AG"],
  "years": [2019],
  "units": []
}
"""
    prompt = f"""
Ты text-to-SQL бот. Твоя задача — сгенерировать SQL-запрос и структурированный JSON-ответ на основе ТОЛЬКО предоставленной схемы и каталога для таблицы с финансовыми данными немецких компаний.
- Если ты уверен больше чем на 80%, что можешь составить точный SQL-запрос, сгенерируй JSON с этим запросом.
- Если ты НЕ УВЕРЕН, или запрос нерелевантен, или нужной информации нет в схеме/каталоге, ты ОБЯЗАН вернуть JSON, где ключ "sql" имеет значение null.

Вот вопрос от пользователя:
"{user_query}"

**КРИТИЧЕСКИ ВАЖНЫЕ ПРАВИЛА ГЕНЕРАЦИИ SQL:**
1.  **ИСПОЛЬЗУЙ ТОЛЬКО ТАБЛИЦУ `{table_name}`**.
2.  **ЭКРАНИРОВАНИЕ КОЛОНОК ОБЯЗАТЕЛЬНО**: Названия колонок в этой таблице содержат пробелы и спецсимволы (например, `Net Income`, `ROA (%)`). Ты **ОБЯЗАН** заключать КАЖДОЕ название колонки в двойные кавычки.
3.  **СТРУКТУРА ТАБЛИЦЫ**: Это "широкая" таблица. Каждая колонка представляет собой отдельный показатель.
4.  **ИСПОЛЬЗУЙ СХЕМУ**: Используй только те колонки, что перечислены в схеме. Не придумывай новые.
    Схема:
    {json.dumps(schema['columns'], indent=2, ensure_ascii=False)}
5.  **ИСПОЛЬЗУЙ КАТАЛОГ**: Для фильтрации в `WHERE` используй официальные названия из каталога. Если пользователь пишет "БМВ", в запросе должно быть `WHERE "Company" = 'BMW AG'`.
    Каталог:
    {json.dumps(catalog, indent=2, ensure_ascii=False)}
6.  **ФИЛЬТРАЦИЯ ПО ДАТАМ**: Колонка "Period" — это текст (например, '12/31/2023'). Для фильтрации по году используй оператор `LIKE`. Пример для 2022 года: `WHERE "Period" LIKE '%2022'`.
7.  **АГРЕГАЦИЯ**: Если пользователь просит сумму, среднее или максимум, используй `SUM()`, `AVG()`, `MAX()`. Если используешь агрегатную функцию вместе с другой колонкой в `SELECT`, эта колонка **ОБЯЗАТЕЛЬНО** должна быть в `GROUP BY`.
8.  **ГОДОВЫЕ СУММЫ**: Если пользователь спрашивает финансовый показатель (как Выручка или Прибыль) за целый год, не указывая квартал, он почти всегда хочет видеть **общую годовую сумму**. В этом случае используй `SUM()` для этого показателя и фильтруй по году через `LIKE`.

Вот хорошие примеры для таблицы `{table_name}`:
{few_shot_examples}

Сформируй ответ в виде строгого JSON со следующими полями:
- `sql` (итоговый SQL-запрос или null)
- `clarified_prompt` (уточненная и полная переформулировка запроса пользователя)
- `metrics` (список ключей метрик из схемы, если есть в запросе)
- `groups` (список ключей групп из каталога, если есть в запросе)
- `years` (список лет, если есть в запросе)
- `units` (список единиц измерения, если есть в запросе)

Верни ТОЛЬКО JSON объект и ничего больше.
"""
    llm_response_str = await get_llm_completion(
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0
    )
    
    try:
        json_match = re.search(r'\{.*\}', llm_response_str, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
            return json.loads(json_str)
        else:
            logger.warning(f"LLM не вернула валидный JSON. Ответ: {llm_response_str}")
            return {"sql": None}
    except (json.JSONDecodeError, IndexError):
        logger.error(f"Ошибка парсинга JSON от LLM. Ответ: {llm_response_str}")
        raise ValueError("Модель вернула некорректный ответ.")

def validate_sql(sql_query: str):
    """Простая синтаксическая проверка SQL."""
    try:
        parsed = sqlparse.parse(sql_query)
        if not parsed or not parsed[0].tokens:
            raise ValueError("SQL-запрос пустой или нераспознан.")
        logger.info("Синтаксис SQL-запроса прошел базовую проверку.")
    except (SQLParseError, ValueError) as e:
        logger.warning(f"Синтаксическая ошибка в сгенерированном SQL: {e}\nЗапрос: {sql_query}")
        raise ValueError("Сгенерирован некорректный SQL-запрос.")

def execute_sql(sql_query: str) -> pd.DataFrame:
    """Выполняет SQL-запрос и возвращает результат в виде DataFrame."""
    try:
        with db_engine.connect() as conn:
            df = pd.read_sql(text(sql_query), conn)
        return df
    except Exception as e:
        logger.error(f"Ошибка выполнения SQL-запроса: {sql_query}\nОшибка: {e}")
        raise IOError("Произошла ошибка при запросе к базе данных.")

async def summarize_result(df: pd.DataFrame, user_query: str) -> str:
    """Формирует итоговый текстовый ответ, используя предварительное форматирование."""
    if df.empty:
        return "По вашему запросу данные не найдены."

    # Форматируем числа для лучшего восприятия моделью и пользователем
    df_formatted = format_numbers_in_df(df.head(15))
    data_for_prompt_string = df_formatted.to_string(index=False)

    prompt = f"""
Ты — ассистент, который формирует краткий и понятный текстовый ответ на русском языке на основе данных из таблицы.
Отвечай строго на основе предоставленных данных, не выдумывай информацию.

Вопрос пользователя: {user_query}
Данные из базы данных (уже отформатированы для удобства):
{data_for_prompt_string}

Твоя задача: Предоставь краткий, человекочитаемый ответ на русском языке.
"""
    answer = await get_llm_completion(
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2
    )
    return answer.strip()

async def run_companies_pipeline(user_query: str) -> str:
    """
    Основной асинхронный пайплайн. Принимает вопрос, возвращает ответ.
    """
    pipeline_name = "companies_pipeline"
    logger.info(f"[{pipeline_name}] Запрос в обработке: '{user_query}'")
    try:
        # 1. Загрузка конфигурации
        schema = load_json(BOT_CONFIG["schema_path"])
        catalog = load_json(BOT_CONFIG["catalog_path"])
        table_name = BOT_CONFIG["table_name_db"]

        # 2. Генерация SQL
        generation_result = await generate_sql(user_query, schema, catalog, table_name)
        sql_query = generation_result.get("sql")

        if not sql_query:
            logger.info(f"[{pipeline_name}] Модель не сгенерировала SQL.")
            return "К сожалению, я не уверен, как точно ответить на ваш вопрос. Пожалуйста, попробуйте переформулировать его."

        logger.info(f"[{pipeline_name}] Сгенерирован SQL: {sql_query}")

        # 3. Валидация и выполнение SQL
        validate_sql(sql_query)
        result_df = execute_sql(sql_query)
        logger.info(f"[{pipeline_name}] Из БД получено строк: {len(result_df)}")

        # 4. Суммаризация результата
        clarified_prompt = generation_result.get("clarified_prompt", user_query)
        answer = await summarize_result(result_df, clarified_prompt)
        
        logger.info(f"[{pipeline_name}] Ответ готов.")
        return answer

    except (ValueError, IOError) as e:
        logger.warning(f"[{pipeline_name}] Ошибка обработки запроса: {e}")
        return str(e)
    except Exception as e:
        logger.exception(f"[{pipeline_name}] Критическая ошибка в пайплайне для запроса: '{user_query}'")
        return "Произошла непредвиденная внутренняя ошибка. Пожалуйста, попробуйте позже."