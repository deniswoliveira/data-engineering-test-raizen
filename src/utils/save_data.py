import logging

import pandas as pd


def save_parquet(df: pd.DataFrame, path: str, partition_cols: list, compression: str = "snappy"):
    df.to_parquet("../".join([path]), partition_cols=[*partition_cols], compression=compression)
    logging.info("File saved!")
