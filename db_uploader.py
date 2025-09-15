import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Загружаем переменные окружения из .env
load_dotenv()

# 1. Load your CSV
df = pd.read_csv("input_data/top_12_german_companies.csv")


DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in .env")

engine = create_engine(DATABASE_URL)


df.to_sql(
    "german_companies",  
    engine,
    if_exists="replace",  
    index=False
)

print("CSV uploaded successfully!")
