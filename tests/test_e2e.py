import os
import pandas as pd 
import pytest 

from src.transformations import transform_data

def test_end_to_end():
    df_raw=pd.read_csv("C:/raw_sales.csv")
    df_clean=transform_data(df_raw)

    sample_row = df_clean.iloc[0]
    assert sample_row["region"] in ["eu", "us", "APAC"]
    assert not df_clean["order_id"].duplicated().any()
    assert (df_clean["quantity"] > 0).all()
    assert (df_clean["price"] > 0).all()