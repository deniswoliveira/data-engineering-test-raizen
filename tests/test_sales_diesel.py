import pandas as pd
import pytest


def read_data(path):
    return pd.read_parquet(path)


def sum_jan2013_sales_diesel(path):
    df = read_data(path)
    df = df[df["year_month"] == "2013-01-01"]
    return int(df["volume"].sum())


def sum_jun2017_sales_diesel(path):
    df = read_data(path)
    df = df[df["year_month"] == "2017-06-01"]
    return int(df["volume"].sum())


def sum_ago2019_sales_diesel(path):
    df = read_data(path)
    df = df[df["year_month"] == "2019-08-01"]
    return int(df["volume"].sum())


def sum_sales_diesel(path):
    df = read_data(path)
    return int(df["volume"].sum())


def test_sum_jan2013_sales_diesel():
    assert sum_jan2013_sales_diesel("/opt/trusted_data/sales_diesel.parquet") == 4456692


def test_sum_jun2017_sales_diesel():
    assert sum_jun2017_sales_diesel("/opt/trusted_data/sales_diesel.parquet") == 4677453


def test_sum_ago2019_sales_diesel():
    assert sum_ago2019_sales_diesel("/opt/trusted_data/sales_diesel.parquet") == 5284080


def test_sum_sales_diesel():
    assert sum_sales_diesel("/opt/trusted_data/sales_diesel.parquet") == 440145528
