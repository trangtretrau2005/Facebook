import logging
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from plugins.modules.facebook.transform import FacebookSparkTransform # Đảm bảo import đúng

# Giả định các import khác
from plugins.config import config
from plugins.helper.facebook_helper import FacebookHelper
from plugins.helper import time_helper
from plugins.helper.gcp_helper import GGSheetHelper, GCSHelper
from plugins.modules.facebook.extract import ExtractFacebookAdCampaignMetrics

local_tz = pendulum.timezone(config.DWH_TIMEZONE)
logger = logging.getLogger('airflow.task')

FB_NAMESPACE = "facebook"
GCS_FOLDER_PATH = [f"gs://{config.BUCKET_NAME}/facebook1/",
    f"gs://{config.BUCKET_NAME}/facebook2/",]
BQ_OUTPUT_TABLE = f"{config.PROJECT_ID}.{config.DATASET_ID}.{config.primary_table_name}"
GCS_SA_KEY_PATH = config.SERVICE_ACCOUNT  # Đường dẫn tới file key JSON"

def extract_fb_to_gcs(namespace, execution_date, table_name, task_name, **kwargs):
    logger.info("🚀 BẮT ĐẦU Extract + Save GCS")

    facebook = FacebookHelper(logger=logger)
    gg_sheet = GGSheetHelper(logger=logger)
    gcs = GCSHelper(logger=logger, bucket_name=config.BUCKET_NAME)

    # Lấy conf
    dag_conf = kwargs.get("dag_run").conf if kwargs.get("dag_run") else {}
    params_conf = kwargs.get("params", {})

    table_config = dag_conf.get(table_name, params_conf.get(table_name))
    if not table_config:
        raise ValueError(f"❌ Không tìm thấy config cho bảng: {table_name}")

    run = table_config.get(task_name, False)

    etl = ExtractFacebookAdCampaignMetrics(
        logger=logger,
        api_client=facebook,
        gg_sheet_client=gg_sheet,
        gcs=gcs,
        table_name=table_name,
        execution_date=execution_date,
        kwargs=kwargs,
    )

    # Hàm run trong ExtractFacebookAdCampaignMetrics cần trả về danh sách các folder GCS đã ghi.
    # Trong trường hợp của bạn, giả định nó trả về list các folder/prefix chứa data:
    # Ví dụ: ['bucket_name/facebook/fb_ad_campaign_metrics/2025-11-15']
    result = etl.run(run=run)

    logger.info("🎉 DONE Save GCS")
    # **Quan trọng:** Trả về đường dẫn thư mục GCS đã ghi để dùng cho bước Transform
    return result if result else [GCS_FOLDER_PATH] # Trả về đường dẫn folder gốc nếu result là None hoặc rỗng


def transform_gcs_to_bq(bq_output_table,sa_key_path, **kwargs):
    logger.info("🚀 BẮT ĐẦU Transform GCS sang BigQuery")
    
    # Lấy đường dẫn GCS từ kết quả của task 'extract_fb_to_gcs'
    ti = kwargs['ti']
    # Giả sử task 'extract_fb_to_gcs' trả về list of GCS folders/prefixes
    gcs_folders = ti.xcom_pull(task_ids='extract_fb_to_gcs')

    if not gcs_folders:
        logger.error("❌ Không tìm thấy GCS folders từ bước extract!")
        raise ValueError("Không có thư mục GCS nào được truyền cho bước transform.")

    # Đảm bảo gcs_folders là list of string (nếu Extract trả về 1 string, wrap nó)
    if isinstance(gcs_folders, str):
        gcs_folders = [gcs_folders]

    # Khởi tạo class transform
    transformer = FacebookSparkTransform(
        logger=logger,
        gcs_folders=GCS_FOLDER_PATH, 
        bq_output_table=bq_output_table,
        service_account_key_path=sa_key_path
    )

    transformer.run(**kwargs)

    logger.info("🎉 DONE Transform GCS sang BigQuery")


def construct_params():
    return {
        config.tmp_table_name: {
            "run": True
        }
    }

default_args = {
    'owner': 'chang',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 3, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='FacebookETL',
    default_args=default_args,
    description='ETL for Facebook data',
    start_date=datetime(2025, 11, 3, 0, tzinfo=local_tz),
    max_active_runs=1,
    schedule="*/30 * * * *" if config.ENV == "prod" else None,
    catchup=False,
    tags=["facebook", "ingestion"],
    params=construct_params()
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # 1. Extract Task
    extract = PythonOperator(
        task_id="extract_fb_to_gcs",
        python_callable=extract_fb_to_gcs,
        op_kwargs={
            "namespace": FB_NAMESPACE,
            "execution_date": time_helper.get_now_local_time(),
            "table_name": config.tmp_table_name,
            "task_name": "run",
            "sheet_id": config.FACEBOOK_AUTO_SHEET_ID,
            "sheet_name": config.FACEBOOK_AUTO_SHEET_NAME
        },
        provide_context=True
    )

    # 2. Transform Task (Mới)
    transform_gcs_to_bq_task = PythonOperator(
        task_id="transform_gcs_to_bq",
        python_callable=transform_gcs_to_bq,
        op_kwargs={
            "bq_output_table": BQ_OUTPUT_TABLE,
            "sa_key_path": GCS_SA_KEY_PATH
        },
        # Đảm bảo context được truyền để có thể dùng XCom pull
        provide_context=True 
    )

    # Định nghĩa luồng DAG: Start -> Extract -> Transform -> End
    start >> extract >> transform_gcs_to_bq_task >> end