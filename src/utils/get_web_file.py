import logging

import requests


def get_web_file(url: str) -> requests.models.Response:
    """Takes a url and return the server's response

    Args:
        url (str): url of the file to download

    Returns:
        requests.models.Response: a server's response to an HTTP request
    """
    logging.info("Getting file form url")
    return requests.get(url)


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
