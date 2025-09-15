import os
import re
import pandas as pd
import json
import time
from pathlib import Path
from tqdm import tqdm
from openai import OpenAI



OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not OPENAI_API_KEY:
    raise ValueError("Missing environment variable: OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY)


#  Папки
CSV_FOLDER = './input_data'
OUTPUT_FOLDER = './metadata_output'
LOG_FOLDER = './metadata_logs'

Path(OUTPUT_FOLDER).mkdir(parents=True, exist_ok=True)
Path(LOG_FOLDER).mkdir(parents=True, exist_ok=True)

#  Настройки
CATEGORICAL_THRESHOLD = 300
CHUNK_SIZE = 15
RETRY_LIMIT = 5
RETRY_DELAY = 7
SLEEP_BETWEEN_REQUESTS = 2

EXCLUDE_COLUMNS = ['year', 'report_year']


#  Удаление даты из названия файла
def clean_table_name(filename):
    return re.sub(r'(_\d{12})?\.csv$', '', filename)


#  Деление на чанки
def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


#  Загрузка описаний таблиц из JSON
def load_table_descriptions(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        descriptions = json.load(f)

    table_desc_map = {}
    for entry in descriptions:
        table_name = entry["Table Name"].split('.')[-1]
        table_desc_map[table_name] = entry["Description"]

    return table_desc_map


#  Запрос к OpenAI с ретраями
def ask_openai(prompt, filename_tag, model="gpt-3.5-turbo", temperature=0):
    for attempt in range(RETRY_LIMIT):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are a JSON generator. Return only valid JSON. No explanations, no markdown, no comments. JSON only."},
                    {"role": "user", "content": prompt}
                ],
                temperature=temperature
            )
            result = response.choices[0].message.content

            log_file = os.path.join(LOG_FOLDER, f"{filename_tag}_attempt{attempt + 1}.txt")
            with open(log_file, "w", encoding='utf-8') as f:
                f.write(result or "EMPTY RESPONSE")

            if result and result.strip() != "":
                time.sleep(SLEEP_BETWEEN_REQUESTS)
                return result

            print(f" Empty response. Retry {attempt + 1}/{RETRY_LIMIT}")
            time.sleep(RETRY_DELAY)

        except Exception as e:
            print(f" OpenAI error: {e}")
            if attempt < RETRY_LIMIT - 1:
                print(f" Retry in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)

    return None


#  Промпты
def generate_table_prompt(table_name, columns, table_description=None):
    cols_list = "\n".join([f"- {col}" for col in columns])

    desc_part = ""
    if table_description:
        desc_part = f"Table description: {table_description}\n\n"

    return f"""
{desc_part}
Table name: {table_name}

Columns:
{cols_list}

Task:
1. For each column, write:
   - A description (in English)
   - Aliases for Russian (ru), English (en), and Kazakh (kz).

Return JSON like:
{{
  "description": "{table_description or '...'}",
  "columns": {{
    "column_name": {{
      "description": "...",
      "aliases": {{"ru": "...", "en": "...", "kz": "..."}}
    }},
    ...
  }},
  "related_catalogs": []
}}
"""


def generate_catalog_prompt(column_name, unique_values):
    values_list = "\n".join([f"- {val}" for val in unique_values])
    return f"""
Column: {column_name}

Unique values:
{values_list}

Task:
For each value, provide:
- A description (in English)
- Aliases for Russian (ru), English (en), and Kazakh (kz).

Return JSON like:
{{
  "column_name": {{
    "value1": {{
      "description": "...",
      "aliases": {{"ru": "...", "en": "...", "kz": "..."}}
    }},
    ...
  }}
}}
"""


#  Обработка одной таблицы
def process_csv(csv_file, table_descriptions):
    table_name = clean_table_name(csv_file)
    csv_path = os.path.join(CSV_FOLDER, csv_file)

    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
    except:
        df = pd.read_csv(csv_path, encoding='cp1251')

    columns = df.columns.tolist()

    table_description = table_descriptions.get(table_name, None)

    table_prompt = generate_table_prompt(table_name, columns, table_description)
    print(f"\n Генерация описания таблицы: {table_name}")

    response = ask_openai(table_prompt, f"{table_name}_schema")

    if not response:
        print(f" Failed table description for {table_name}")
        return None, None

    try:
        table_schema = json.loads(response)
    except Exception as e:
        print(f" JSON parse error in table {table_name}: {e}")
        print(response)
        return None, None

    catalogs = {}

    #  Каталог компаний
    if {'name_short_ru', 'name_short_en'}.issubset(set(df.columns)):
        company_values = df[['name_short_ru', 'name_short_en']].drop_duplicates()

        if 'name_abbr' in df.columns:
            company_values['name_abbr'] = df['name_abbr']
        else:
            company_values['name_abbr'] = ""

        catalogs['company'] = {}

        for _, row in company_values.iterrows():
            ru = row['name_short_ru']
            en = row['name_short_en']
            abbr = row['name_abbr']

            key = ru

            catalogs['company'][key] = {
                "description": "Company name",
                "aliases": {
                    "ru": ru,
                    "en": en,
                    "abbr": abbr
                }
            }

    #  Поиск категориальных колонок (все кроме исключённых)
    for col in df.columns:
        if col in EXCLUDE_COLUMNS or col in ['name_short_ru', 'name_short_en', 'name_abbr']:
            continue

        nunique = df[col].nunique(dropna=True)
        if nunique <= CATEGORICAL_THRESHOLD:
            unique_values = df[col].dropna().astype(str).unique().tolist()
            combined_catalog = {}

            chunks = list(chunk_list(unique_values, CHUNK_SIZE))

            for idx, chunk in enumerate(chunks):
                print(f" Генерация каталога {table_name}.{col} — чанк {idx + 1}/{len(chunks)}")

                prompt = generate_catalog_prompt(col, chunk)
                tag = f"{table_name}_{col}_chunk{idx + 1}"

                retry_success = False

                for attempt in range(RETRY_LIMIT):
                    cat_response = ask_openai(prompt, tag)

                    if cat_response and cat_response.strip() != "":
                        try:
                            cat_json = json.loads(cat_response)
                            combined_catalog.update(cat_json.get(col, {}))

                            chunk_file = os.path.join(LOG_FOLDER, f'{tag}_output.json')
                            with open(chunk_file, 'w', encoding='utf-8') as f:
                                json.dump(cat_json, f, indent=2, ensure_ascii=False)

                            retry_success = True
                            break

                        except Exception as e:
                            print(f" JSON parse error in {col} chunk {idx + 1} attempt {attempt + 1}: {e}")
                            time.sleep(RETRY_DELAY)
                    else:
                        print(f" Empty response for {col} chunk {idx + 1} attempt {attempt + 1}")
                        time.sleep(RETRY_DELAY)

                if not retry_success:
                    print(f" Failed to process {col} chunk {idx + 1} after {RETRY_LIMIT} retries")

            if combined_catalog:
                catalogs[col] = combined_catalog

    return table_schema, catalogs


#  Основной цикл
def main():
    table_descriptions = load_table_descriptions(os.path.join(CSV_FOLDER, 'table_descriptions.json'))

    csv_files = [f for f in os.listdir(CSV_FOLDER) if f.endswith('.csv')]

    for csv_file in tqdm(csv_files, desc="Processing CSV"):
        table_name = clean_table_name(csv_file)
        table_schema, catalogs = process_csv(csv_file, table_descriptions)

        if table_schema:
            related_catalogs = [f"{col}_catalog.json" for col in catalogs.keys()] if catalogs else []
            table_schema["related_catalogs"] = related_catalogs

            schema_path = os.path.join(OUTPUT_FOLDER, f'{table_name}_schema.json')
            with open(schema_path, 'w', encoding='utf-8') as f:
                json.dump(table_schema, f, indent=2, ensure_ascii=False)

        if catalogs:
            catalog_path = os.path.join(OUTPUT_FOLDER, f'{table_name}_catalog.json')
            with open(catalog_path, 'w', encoding='utf-8') as f:
                json.dump(catalogs, f, indent=2, ensure_ascii=False)

    print("\n Схема и каталоги успешно сохранены по таблицам!")


if __name__ == "__main__":
    main()
