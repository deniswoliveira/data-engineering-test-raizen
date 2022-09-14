import locale
import logging
import os
import subprocess
from unicodedata import normalize

import numpy as np
import pandas as pd

locale.setlocale(locale.LC_ALL, "pt_BR.utf8")


def create_dir(dir: str):
    """Takes a path and created the directory

    Args:
        dir (string): path to be created
    """
    logging.info(f"Creating folder on path {dir}")
    os.makedirs(dir, exist_ok=True)


def convert_file_to_xls(path: str, out_dir: str) -> str:
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
    return output


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
