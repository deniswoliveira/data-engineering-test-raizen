import glob

import pandas as pd

path = "/opt/trusted_data/sales_diesel/*/*.parquet"


def read_data(path):
    files = glob.glob(path)
    data = [pd.read_parquet(f) for f in files]
    return pd.concat(data, ignore_index=True)


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
    assert sum_jan2013_sales_diesel(path) == 4456692


def test_sum_jun2017_sales_diesel():
    assert sum_jun2017_sales_diesel(path) == 4677453


def test_sum_ago2019_sales_diesel():
    assert sum_ago2019_sales_diesel(path) == 5284080


def test_sum_sales_diesel():
    assert sum_sales_diesel(path) == 440145528
