import pandas as pd
import unittest
from src.transformations import transform_data

def test_valid_data_transormation():
    valid_data = pd.DataFrame({
        "order_id": [1, 2, 3, 4],
        "order_date": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
        "customer_id": [101, 102, 103, 104],
        "product_id": [201, 202, 203, 204],
        "quantity": [1, 2, 3, 4],
        "price": [10.0, 20.0, 30.0, 40.0],
        "region": ["eu", "us", "APAC", "eu"]
    })

    transformed = transform_data(valid_data)
    assert len(transformed) == 4
    assert all(transformed["region"].isin(["eu", "us", "APAC"]))

def test_invalid_region():
    invalid_data = pd.DataFrame({
        "order_id": [5, 6],
        "order_date": ["2023-01-05", None],
        "customer_id": [105, 106],
        "product_id": [205, 206],
        "quantity": [5, None],
        "price": [50.0, None],
        "region": ["invalid_region", "eu"]
    })

    transformed = transform_data(invalid_data)
    assert len(transformed) == 1

def test_duplicates_removal():
    duplicate_data = pd.DataFrame({
        "order_id": [11, 11],
        "order_date": ["2023-01-07", "2023-01-08"],
        "customer_id": [111, 112],
        "product_id": [211, 212],
        "quantity": [9, 10],
        "price": [90.0, 100.0],
        "region": ["eu", "us"]
    })
    
    transformed = transform_data(duplicate_data)
    assert len(transformed) == 1  # Должна остаться только одна запись

if __name__ == "__main__":
    unittest.main()