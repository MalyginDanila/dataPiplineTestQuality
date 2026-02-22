import pandas as pd
from typing import List

VALID_REGIONS=["eu", "us", "APAC"]

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    #Проверяем, что есть валидные регионы
    df = df[df["region"].isin(VALID_REGIONS)]

    #Удаляем плохие записи, в которых есть пропуски
    df = df.dropna(
        subset=["order_id", "order_date", "customer_id", "product_id", "quantity", "price"]
    )

    #Сортируем по дате
    df = df.sort_values("order_date")

    #Удаляем дубликаты
    df = df.drop_duplicates(subset=["order_id"], keep="first")

    return df.reset_index(drop=True)