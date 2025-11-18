import os

import yaml
import dotenv
dotenv.load_dotenv('/opt/airflow/secrets/.env')


class AppConfig(object):
    """
    Access environment variables here.
    """

    def __init__(self):
        """
        Load secret to config   
        """
        self.lark_base_config = None
    ENV = os.getenv("ENV", "dev")
    INIT_DATE = "0000/00/00"
    INIT_HOUR = "/24"

    LOG_LEVEL = os.getenv("LOG_LEVEL", "debug")
    PROJECT_ID = os.getenv("PROJECT_ID", "unknown")
    DATASET_ID = os.getenv("DATASET_ID", "unknown")
    DATASET_STAGING_ID = os.getenv("DATASET_STAGING_ID", "staging")
    SERVICE_ACCOUNT = os.getenv("SERVICE_ACCOUNT", "unknown")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "unknown")
    PREFIX_JSON_NAME = "json"
    primary_table_name = "facebook_ad_campaign_metrics"
    tmp_table_name = f"{primary_table_name}_tmp"
    FACEBOOK_BASE_URL = os.getenv("FACEBOOK_BASE_URL", "https://graph.facebook.com")
    FACEBOOK_API_VERSION = os.getenv("FACEBOOK_API_VERSION", "v24.0")
    FACEBOOK_API_TIMEOUT = os.getenv("FACEBOOK_API_TIMEOUT", 300)
    FACEBOOK_POOL_CONNECTIONS = os.getenv("FACEBOOK_POOL_CONNECTIONS", 50)
    FACEBOOK_POOL_MAX_SIZE = os.getenv("FACEBOOK_POOL_MAX_SIZE", 100)
    FACEBOOK_PAGE_METRICS_LIMIT = os.getenv("FACEBOOK_PAGE_METRICS_LIMIT", 400)
    FACEBOOK_PAGE_METRICS_RETENTION_DAYS = os.getenv("FACEBOOK_PAGE_METRICS_RETENTION_DAYS", 729)
    FACEBOOK_PAGE_METRICS_RANGE_DAYS = os.getenv("FACEBOOK_PAGE_METRICS_RANGE_DAYS", 92)
    FACEBOOK_AUTO_SHEET_ID = os.getenv("FACEBOOK_AUTO_SHEET_ID", "unknown")
    FACEBOOK_AUTO_SHEET_NAME = os.getenv("FACEBOOK_AUTO_SHEET_NAME", "unknown")

    GG_SHEET_BASE_URL = os.getenv("GGSHEET_BASE_URL", "https://docs.google.com/spreadsheets")

    USER_AGENT = os.getenv("USER_AGENT","Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3")

    DWH_TIMEZONE = 'Asia/Ho_Chi_Minh'
    DWH_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    NEW_DATA = "has_new_data"

config = AppConfig()
