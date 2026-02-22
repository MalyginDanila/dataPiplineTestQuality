from datetime import datetime, timedelta
import os
import pandas as pd

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable

from great_expectations.dataset import PandasDataset

from src.transformations import transform_sales_data

DEFAULT_ARGS = {
    "owner": "dataops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def load_validate_file():
    df = pd.read_csv('C:/raw_sales.csv')
    
    #схема файла
    expected_columns = [
        "order_id",
        "order_date",
        "customer_id",
        "product_id",
        "quantity",
        "price",
        "region"
    ]

    #Проверяем данные в датафрейме на сопоставление со схемой
    if list(df.columns) != expected_columns:
        raise ValueError(f"Неправильные значения: {df.columns}")
    
    ds = PandasDataset(df)

    #Проверяем, что данные не нулевые
    ds.expect_column_values_to_not_be_null("order_id")
    ds.expect_column_values_to_not_be_null("order_date")
    ds.expect_column_values_to_not_be_null("customer_id")
    ds.expect_column_values_to_not_be_null("product_id")
    ds.expect_column_values_to_not_be_null("quantity")
    ds.expect_column_values_to_not_be_null("price")
    ds.expect_column_values_to_not_be_null("region")

    #Проверяем тип данных
    ds.expect_column_values_to_of_type("order_id", "int_64")
    ds.expect_column_values_to_of_type("order_date", "date")
    ds.expect_column_values_to_of_type("quantity", "int_64")
    ds.expect_column_values_to_of_type("price", "float_64")

    result = ds.validate()
    if not result["success"]:
        raise ValueError("Проверки не сошлись")
    
    return df


def validate_clean_data():
    
    df = load_validate_file()
    
    if df["order_id"].duplicated().any():
        raise ValueError("Duplicate order_id in clean data")
    
    if (df["quantity"] <= 0).any():
        raise ValueError("Quantity must be > 0")
    
    if (df["price"] <= 0).any():
        raise ValueError("Price must be > 0")
    
    allowed_regions = {"EU", "US", "APAC"}
    if not set(df["region"]).issubset(allowed_regions):
        raise ValueError("Unexpected region values")

    

with DAG(
    dag_id="ingress_data_quality_cheks",
    start_date=datetime(2026, 3, 1),
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["dataQuality"],
) as dag:

    t_load_validate_file = PythonOperator(
        task_id = "load_validate_data",
        python_callable = load_validate_file,
        provide_context = True
    )

    t_validate_clean = PythonOperator(
        task_id = "validate_clean_data",
        python_callable = validate_clean_data,
        provide_context = True
    )

    t_load_validate_file >> t_validate_clean