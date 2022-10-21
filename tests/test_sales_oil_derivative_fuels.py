import glob

import pandas as pd

path = "/opt/trusted_data/sales_oil_derivative_fuels/*/*.parquet"


def read_data(path):
    files = glob.glob(path)
    data = [pd.read_parquet(f) for f in files]
    return pd.concat(data, ignore_index=True)


def sum_jan2000_sales_oil_derivative_fuels(path):
    df = read_data(path)
    df = df[df["year_month"] == "2000-01-01"]
    return int(df["volume"].sum())


def sum_jun2011_sales_oil_derivative_fuels(path):
    df = read_data(path)
    df = df[df["year_month"] == "2011-06-01"]
    return int(df["volume"].sum())


def sum_ago2013_sales_oil_derivative_fuels(path):
    df = read_data(path)
    df = df[df["year_month"] == "2013-08-01"]
    return int(df["volume"].sum())


def sum_sales_oil_derivative_fuels(path):
    df = read_data(path)
    return int(df["volume"].sum())


def test_sum_jan2000_sales_oil_derivative_fuels():
    assert sum_jan2000_sales_oil_derivative_fuels(path) == 6995110


def test_sum_jun2011_sales_oil_derivative_fuels():
    assert sum_jun2011_sales_oil_derivative_fuels(path) == 10184502


def test_sum_ago2013_sales_oil_derivative_fuels():
    assert sum_ago2013_sales_oil_derivative_fuels(path) == 12082456


def test_sum_sales_oil_derivative_fuels():
    assert sum_sales_oil_derivative_fuels(path) == 2369227066
