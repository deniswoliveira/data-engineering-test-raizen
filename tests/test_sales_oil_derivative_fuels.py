import pandas as pd
import pytest


def read_data(path):
    return pd.read_parquet(path)


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
    assert (
        sum_jan2000_sales_oil_derivative_fuels(
            "/opt/trusted_data/sales_oil_derivative_fuels.parquet"
        )
        == 6995110
    )


def test_sum_jun2011_sales_oil_derivative_fuels():
    assert (
        sum_jun2011_sales_oil_derivative_fuels(
            "/opt/trusted_data/sales_oil_derivative_fuels.parquet"
        )
        == 10184502
    )


def test_sum_ago2013_sales_oil_derivative_fuels():
    assert (
        sum_ago2013_sales_oil_derivative_fuels(
            "/opt/trusted_data/sales_oil_derivative_fuels.parquet"
        )
        == 12082456
    )


def test_sum_sales_oil_derivative_fuels():
    assert (
        sum_sales_oil_derivative_fuels("/opt/trusted_data/sales_oil_derivative_fuels.parquet")
        == 2369227066
    )
