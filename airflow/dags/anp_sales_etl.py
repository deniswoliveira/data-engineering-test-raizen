import locale
import logging
import logging.config
import subprocess
import sys
from os import makedirs, path
from unicodedata import normalize

import numpy as np
import pandas as pd

locale.setlocale(locale.LC_ALL, "pt_BR.utf8")

sys.path.append("/opt/airflow")
import logging

import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def normalize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Takes a dataframe and normalizes its columns

    Args:
        df (Dataframe): dataframe to be normalized

    Returns:
        Dataframe: dataframe with normalized columns
    """
    return [
        normalize("NFKD", col).encode("ASCII", "ignore").decode("ASCII").lower()
        for col in df.columns
    ]


def shift_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Receive a dataframe and iterate correcting data shift

    Args:
        df (DataFrame): dataframe to be fixed

    Returns:
        Dataframe: dataframe with normalized data
    """
    index = -1
    TOTAL_COLUMNS = (len(df.columns) - 1) * -1
    for row_index, row in df.iterrows():
        index = index + 13 if index < TOTAL_COLUMNS else index
        dados = row.values.flatten().tolist()
        dados = np.roll(dados, index)
        df.iloc[row_index, :] = dados
        index = -1 if index < TOTAL_COLUMNS else index - 1
    return df


def normalize_shifted_data(df: pd.DataFrame, features: list, values: list) -> pd.DataFrame:
    """_summary_

    Args:
        df (DataFrame): Dataframe containing incorrect data
        features (list): feature columns
        values (list): values columns

    Returns:
        DataFrame: dataframe with normalized data
    """
    df_features = df[features]
    df_values = shift_dataset(df[values])
    return pd.concat([df_features, df_values], axis=1)


def normalize_anp_fuel_sales_data(df: pd.DataFrame, features: list, values: list) -> pd.DataFrame:
    """Takes dataframe containing data from anp fuel sales and normalizes it for analysis

    Args:
        df (pd.DataFrame): dataframe containing data from anp fuel sales
        features (list): dataframe feature column list
        values (list): dataframe values column list

    Returns:
        DataFrame: dataframe with normalized data and core columns
    """
    df.columns = normalize_df_columns(df)
    df = normalize_shifted_data(df, features, values)
    df = df.melt(id_vars=features)
    df = df.loc[df["variable"] != "total"]
    df["created_at"] = pd.Timestamp.today()
    df["year_month"] = pd.to_datetime(df["ano"].astype(str) + "-" + df["variable"], format="%Y-%b")
    df["product"], df["unit"] = (
        df["combustivel"].str.split("(").str[0].str.strip(),
        df["combustivel"]
        .str.split("(")
        .str[1]
        .replace(to_replace="\\)", value="", regex=True)
        .str.strip(),
    )
    df = df.drop(labels=["variable", "regiao", "ano", "combustivel"], axis=1)
    df.rename(columns={"estado": "uf", "value": "volume"}, inplace=True)
    df.fillna(0, inplace=True)
    return df


def read_excel(path: str, sheet: str) -> pd.DataFrame:
    """Receives input path from an excel and returns a loaded dataframe

    Args:
        path (str): input path for a excel file
        sheet (str): sheet to be read in excel

    Returns:
        DataFrame: loaded dataframe
    """
    return pd.read_excel(path, sheet_name=sheet)


def transform_anp(new_filepath, sheet, features, values, dest_path, dest_filename):
    logging.info("Reading excel file")
    df = read_excel(new_filepath, sheet)
    df = normalize_anp_fuel_sales_data(df, features, values)
    create_dir(dest_path)
    df.to_parquet(dest_path + dest_filename)


def convert_file_to_xls(path: str, out_dir: str) -> None:
    """Takes a path to an xls file and converts using libreoffice to the destination directory

    Args:
        path (string): path to a xls file
        out_dir (string): path to output de conversion

    Returns:
        string: conversion result
    """
    logging.info("Converting the file")
    bash_command = f"libreoffice --headless --convert-to xls --outdir {out_dir} {path}"
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    if error:
        logging.error("Error converting file ro xls")
        raise
    logging.info("File converted!")


def save_web_file(file: bytes, dest_file: str):
    """Takes a file and write on the destination path

    Args:
        file (bytes): content of a file
        dest_file (str): destination of the file to be written
    """
    try:
        logging.info(f"Writing file on path {dest_file}")
        with open(dest_file, "wb") as f:
            f.write(file)
    except Exception as e:
        logging.error(f"Error writing file - {e}")
        raise


def get_web_file(url: str) -> requests.models.Response:
    """Takes a url and return the server's response

    Args:
        url (str): url of the file to download

    Returns:
        requests.models.Response: a server's response to an HTTP request
    """
    logging.info("Getting file form url")
    return requests.get(url)


def create_dir(dir: str):
    """Takes a path and created the directory

    Args:
        dir (string): path to be created
    """
    logging.info(f"Creating folder on path {dir}")
    makedirs(dir, exist_ok=True)


def download_anp_fuel_sales(url, raw_path):
    logger = logging.getLogger(__name__)
    logger.info("Defining output variables")
    filename = url.split("/")[-1]
    filepath = "/".join([raw_path, filename])

    logger.info("Starting the file download process")
    create_dir(raw_path)
    web_file = get_web_file(url)
    save_web_file(web_file.content, filepath)


default_args = {
    "owner": "Denis Oliveira",
    "start_date": days_ago(1),
    "retries": 0,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="download_anp_fuel_sales_id",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@once",
    description="anp fuel sales",
    catchup=False,
) as dag:
    # Task 1 - Download anp fuel sales from github
    task_download_anp_fuel_sales = PythonOperator(
        task_id="download_anp_fuel_sales",
        python_callable=download_anp_fuel_sales,
        dag=dag,
        op_kwargs={
            "url": "https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls",
            "raw_path": "/home/airflow/raw_data",
        },
    )

    # Task 2 - Converting dynamic table in sheets anp fuel sales
    task_converting_file_to_xls = PythonOperator(
        task_id="converting_file_to_xls",
        python_callable=convert_file_to_xls,
        dag=dag,
        op_kwargs={
            "path": "/home/airflow/raw_data/vendas-combustiveis-m3.xls",
            "out_dir": "/home/airflow/processed_data/",
        },
    )

    # Task 3 - Normalize sales oil derivative fuels
    task_transform_anp_sales_oil_derivative_fuels = PythonOperator(
        task_id="transform_anp_sales_oil_derivative_fuels",
        python_callable=transform_anp,
        dag=dag,
        op_kwargs={
            "new_filepath": "/home/airflow/processed_data/vendas-combustiveis-m3.xls",
            "sheet": "DPCache_m3",
            "dest_path": "/home/airflow/trusted/",
            "dest_filename": "sales_oil_derivative_fuels.parquet",
            "features": ["combustivel", "ano", "regiao", "estado"],
            "values": [
                "jan",
                "fev",
                "mar",
                "abr",
                "mai",
                "jun",
                "jul",
                "ago",
                "set",
                "out",
                "nov",
                "dez",
                "total",
            ],
        },
    )

    # Task 4 - Normalize sales diesel
    task_transform_anp_sales_diesel = PythonOperator(
        task_id="transform_anp_sales_diesel",
        python_callable=transform_anp,
        dag=dag,
        op_kwargs={
            "new_filepath": "/home/airflow/processed_data/vendas-combustiveis-m3.xls",
            "sheet": "DPCache_m3_2",
            "dest_path": "/home/airflow/trusted/",
            "dest_filename": "sales_diesel.parquet",
            "features": ["combustivel", "ano", "regiao", "estado"],
            "values": [
                "jan",
                "fev",
                "mar",
                "abr",
                "mai",
                "jun",
                "jul",
                "ago",
                "set",
                "out",
                "nov",
                "dez",
                "total",
            ],
        },
    )

    (
        task_download_anp_fuel_sales
        >> task_converting_file_to_xls
        >> [task_transform_anp_sales_oil_derivative_fuels, task_transform_anp_sales_diesel]
    )
