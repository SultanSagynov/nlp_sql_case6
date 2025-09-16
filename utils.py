# utils.py
import pandas as pd

def format_numbers_in_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Принимает DataFrame и форматирует числовые колонки для человекочитаемого вида.
    """
    df_copy = df.copy()
    
    for col in df_copy.columns:
        if not pd.api.types.is_numeric_dtype(df_copy[col]):
            continue

        def format_value(x):
            if pd.isna(x):
                return x
            try:
                num = float(x)
                if abs(num) >= 1_000_000_000:
                    val = f'{num / 1_000_000_000:,.2f}'.replace(',', ' ').replace('.00', '')
                    return f'{val} млрд евро'
                if abs(num) >= 1_000_000:
                    val = f'{num / 1_000_000:,.2f}'.replace(',', ' ').replace('.00', '')
                    return f'{val} млн евро'
                return f'{num:,.2f}'.replace(',', ' ').replace('.00', '') + ' €'
            except (ValueError, TypeError):
                return x

        df_copy[col] = df_copy[col].apply(format_value).astype(str)
            
    return df_copy