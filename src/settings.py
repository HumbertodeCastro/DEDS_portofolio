from pathlib import Path
from loguru import logger

class Settings():
    basedir = Path.cwd()
    rawdir = Path("raw_data")
    processeddir = Path("processed_data")
    logdir = basedir / "log"

    SQL_SERVER_DB = {
        'servername': 'LAPTOP-LPE28RPE\SQLEXPRESS',
        'database': 'DEDS',
        'driver': 'SQL Server',
    }

    SQLITE_DBS = {
        'go_sales_sqlite': 'Great_Outdoors_Data_SQLite\\go_sales.sqlite',
        'go_crm_sqlite': 'Great_Outdoors_Data_SQLite\\go_crm.sqlite',
        'go_staff_sqlite': 'Great_Outdoors_Data_SQLite\\go_staff.sqlite',
    }

    CSV_PATHS = {
        'go_inv_con': 'Great_Outdoors_Data_SQLite\\GO_SALES_INVENTORY_LEVELSData.csv',
        'go_forecast_con': 'Great_Outdoors_Data_SQLite\\GO_SALES_PRODUCT_FORECASTData.csv',
    }


settings = Settings()
logger.add("logfile.log")