import logging
import logging.config
from os import path

from dotenv import load_dotenv

from utils.data_transform import *
from utils.get_web_file import get_web_file, save_web_file
from utils.read_data import read_excel
from utils.save_data import save_parquet

if __name__ == "__main__":
    log_file_path = path.join(path.dirname(path.abspath(__file__)), "logging_config.ini")
    logging.config.fileConfig(log_file_path)

    logger = logging.getLogger(__name__)
    logger.info("Starting process")

    logger.info("Loading environment variables")
    load_dotenv()
    url = os.getenv("URL")
    raw_path = os.getenv("BRONZE_STAGE")
    stage_path = os.getenv("SILVER_STAGE")
    trusted_path = os.getenv("GOLD_STAGE")
    table_conf = eval(os.getenv("EXCEL_CONF"))
    features = eval(os.getenv("EXCEL_FEATURES"))
    values = eval(os.getenv("EXCEL_VALUES"))

    logger.info("Defining output variables")
    filename = url.split("/")[-1]
    filepath = "/".join([raw_path, filename])
    new_filepath = "/".join([stage_path, filename])

    logger.info("Starting the file download process")
    create_dir(raw_path)
    web_file = get_web_file(url)
    save_web_file(web_file.content, filepath)

    logger.info("Starting the file conversion process")
    output = convert_file_to_xls(filepath, stage_path)

    logger.info("Creating trusted path")
    create_dir(trusted_path)
    for sheet in table_conf:
        logger.info("Starting the file transformation process")
        df = read_excel(new_filepath, sheet)
        df = normalize_anp_fuel_sales_data(df, features, values)

        logger.info("Starting the file saving process")
        save_parquet(
            df,
            "/".join([trusted_path, table_conf[sheet]["table_name"]]),
            table_conf[sheet]["partitions"],
            "snappy",
        )
