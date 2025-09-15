import asyncio
import argparse
import sys

# --- Импортируем основной пайплайн из нашего бота ---
from pipeline import run_companies_pipeline
from config import logger

async def main():
    """
    Асинхронная главная функция для запуска пайплайна из командной строки.
    """
    # Настраиваем парсер аргументов командной строки
    parser = argparse.ArgumentParser(
        description="CLI для тестирования пайплайна чат-бота."
    )
    parser.add_argument(
        "query",
        type=str,
        help="Текстовый запрос к боту в кавычках."
    )
    
    # Если аргументы не переданы, выводим справку и выходим
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
        
    args = parser.parse_args()
    
    user_query = args.query
    
    if not user_query:
        logger.error("Запрос не может быть пустым.")
        return

    logger.info(f"--- Запуск теста CLI с запросом: '{user_query}' ---")
    
    try:
        # Вызываем наш основной пайплайн
        answer = await run_companies_pipeline(user_query)
        
        # Печатаем результат в консоль
        print("\n" + "="*50)
        print("Ответ Бота:")
        print("="*50)
        print(answer)
        print("="*50)
        
    except Exception as e:
        logger.exception(f"Во время выполнения теста CLI произошла критическая ошибка: {e}")

if __name__ == "__main__":
    # Запускаем асинхронную функцию main
    asyncio.run(main())