import pandas as pd


def read_excel(path: str, sheet: str) -> pd.DataFrame:
    """Receives input path from an excel and returns a loaded dataframe

    Args:
        path (str): input path for a excel file
        sheet (str): sheet to be read in excel

    Returns:
        DataFrame: loaded dataframe
    """
    return pd.read_excel(path, sheet_name=sheet)
