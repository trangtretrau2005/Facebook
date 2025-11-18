from logging import Logger

import gspread
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from google.oauth2.service_account import Credentials
from requests.exceptions import HTTPError, RequestException

from plugins.config import config

TBL_FB_AD_CAMPAIGN_METRICS_RECORD1 = {
    "account_currency": "string",
    "account_id": "string",
    "account_name": "string",
    "action_values": "string",
    "actions": "string",
    "ad_click_actions": "string",
    "ad_id": "string",
    "ad_impression_actions": "string",
    "ad_name": "string",
    "adset_id": "string",
    "adset_name": "string",
    "attribution_setting": "string",
    "auction_bid": "string",
    "auction_competitiveness": "string",
    "auction_max_competitor_bid": "string",
    "buying_type": "string",
    "campaign_id": "string",
    "campaign_name": "string",
    "canvas_avg_view_percent": "string",
    "canvas_avg_view_time": "string",
    "catalog_segment_actions": "string",
    "catalog_segment_value": "string",
    "catalog_segment_value_mobile_purchase_roas": "string",
    "catalog_segment_value_omni_purchase_roas": "string",
    "catalog_segment_value_website_purchase_roas": "string",
    "clicks": "string",
    "conversion_values": "string",
    "conversions": "string",
    "converted_product_quantity": "string",
    "converted_product_value": "string",
    "cost_per_15_sec_video_view": "string",
    "cost_per_2_sec_continuous_video_view": "string",
    "cost_per_action_type": "string",
    "cost_per_ad_click": "string",
    "cost_per_conversion": "string",
    "cost_per_dda_countby_convs": "string",
    "cost_per_inline_link_click": "string",
    "cost_per_inline_post_engagement": "string",
    "cost_per_one_thousand_ad_impression": "string",
    "cost_per_outbound_click": "string",
    "cost_per_thruplay": "string",
    "cost_per_unique_action_type": "string",
    "cost_per_unique_click": "string",
    "cost_per_unique_conversion": "string",
    "cost_per_unique_inline_link_click": "string",
    "cost_per_unique_outbound_click": "string",
    "cpc": "string",
    "cpm": "string",
    "cpp": "string",
    "created_time": "datetime",
    "ctr": "string",
    "date_start": "datetime",
    "date_stop": "datetime",
    "dda_countby_convs": "string",
    "dda_results": "string",
    "frequency": "string",
    "full_view_impressions": "string",
    "full_view_reach": "string",
    "impressions": "string",
    "inline_link_click_ctr": "string",
    "inline_link_clicks": "string",
    "inline_post_engagement": "string",
    "instagram_upcoming_event_reminders_set": "string",
    "instant_experience_clicks_to_open": "string",
    "instant_experience_clicks_to_start": "string",
    "instant_experience_outbound_clicks": "string",
    "interactive_component_tap": "string",
    "marketing_messages_delivery_rate": "string",
    "mobile_app_purchase_roas": "string",
    "objective": "string",
    "optimization_goal": "string",
    "outbound_clicks": "string",
    "outbound_clicks_ctr": "string",
    "place_page_name": "string",
    "purchase_roas": "string",
    "qualifying_question_qualify_answer_rate": "string",
    "reach": "string",
    "social_spend": "string",
    "spend": "string",
    "updated_time": "string",
    "video_30_sec_watched_actions": "string",
    "video_avg_time_watched_actions": "string",
    "video_continuous_2_sec_watched_actions": "string",
    "video_p100_watched_actions": "string",
    "video_p25_watched_actions": "string",
    "video_p50_watched_actions": "string",
    "video_p75_watched_actions": "string",
    "video_p95_watched_actions": "string",
    "video_play_actions": "string",
    "video_play_curve_actions": "string",
    "video_play_retention_0_to_15s_actions": "string",
    "video_play_retention_20_to_60s_actions": "string",
    "video_play_retention_graph_actions": "string",
    "video_time_watched_actions": "string",
    "website_ctr": "string",
    "website_purchase_roas": "string"
}


class GGSheetHelper:
    scopes = ['https://www.googleapis.com/auth/drive']

    def __init__(self, logger: Logger, client=None, credentials_file=None):
        self.client = client
        self.logger = logger
        if not self.client:
            self.credentials = Credentials.from_service_account_file(credentials_file or config.SERVICE_ACCOUNT,
                                                                     scopes=GGSheetHelper.scopes)
            self.client = gspread.authorize(credentials=self.credentials)

    def get_sheet_data(self, sheet_id, sheet_name, body_offset=1):
        sheet_url = f"{config.GG_SHEET_BASE_URL}/d/{sheet_id}"

        try:
            self.logger.info(f"Get data from GG Sheet with sheet_id: {sheet_id}, sheet_name: {sheet_name}")
            sheet_content = self.client.open_by_url(sheet_url).worksheet(sheet_name)
            self.logger.info(sheet_content.get_all_values())

            return sheet_content.get_all_values()[body_offset:]
        except HTTPError as e:
            # raise e
            self.logger.error(f"HTTPError occurred: {e}")
        except RequestException as e:
            error_msg = f"Request exception when getting record from GG sheet with sheet_id: {sheet_id}, sheet_name: {sheet_name}, error: {e}"
            # raise Exception(error_msg)
            self.logger.error(error_msg)
        except Exception as e:
            # raise e
            self.logger.error(f"An unexpected error occurred: {e}")

        return None


class GCSHelper:

    def __init__(self, logger: Logger, client=None, credentials_file=None, bucket_name=None):

        self.client = client
        self.logger = logger
        self.bucket_name = bucket_name
        if not self.client:
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_file or config.SERVICE_ACCOUNT)
            self.client = storage.Client(credentials=self.credentials)
            self.bucket = self.client.get_bucket(self.bucket_name)

    def upload_json(self, json_string, file_name):

        # Get the specified bucket

        # Create a blob with the desired file name
        blob = self.bucket.blob(file_name)

        # Upload the JSON string to the blob
        try:
            blob.upload_from_string(json_string, content_type='application/json; charset=utf-8')
            self.logger.info(f"JSON file uploaded to GCS: gs://{self.bucket_name}/{file_name}")
        except:
            self.logger.error(f"Error when uploading to gs://{self.bucket_name}/{file_name}")

    def download_json(self, blob):
        try:
            json_string = blob.download_as_text()
            self.logger.info(f"JSON file downloaded from GCS: gs://{self.bucket_name}/{blob.name}")
            return json_string
        except:
            self.logger.error(f"Error when downloading from GCS://{self.bucket_name}/{blob.name}")


class BQHelper:

    def __init__(self, logger: Logger, client=None, credentials_file=None):

        self.client = client
        self.logger = logger
        if not self.client:
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_file or config.SERVICE_ACCOUNT,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            self.client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)

    def get_table(self, table_id='project-id.dataset-id.table-id'):

        # TODO(developer): Set table_id to the ID of the model to fetch.
        # table_id = 'your-project.your_dataset.your_table'

        table = self.client.get_table(table_id)  # Make an API request.

        # View table properties
        self.logger.info(
            "Got table '{}.{}.{}'.".format(table.project, table.dataset_id, table.table_id)
        )
        self.logger.info("Table schema: {}".format(table.schema))
        self.logger.info("Table description: {}".format(table.description))
        self.logger.info("Table has {} rows".format(table.num_rows))

        # SELECT * EXCEPT(is_typed)
        # FROM mydataset.INFORMATION_SCHEMA.TABLES

        return table
    def execute(self, query):
        query_job = self.client.query(query)
        results = query_job.result()
        return results

    def select(self, query):
        try:
            data_frame = pandas_gbq.read_gbq(query, credentials=self.credentials, progress_bar_type=None)
        except Exception as e:
            print(e)
            return False
        return data_frame

    def bq_append(self, update_data, table_name, dataset_id, if_exists='append', project_id=config.PROJECT_ID):
        if update_data is None or update_data.shape[0] == 0:
            return False, "Empty DataFrame"
            # update_data=load_data.copy()

        table_id = f'{project_id}.{dataset_id}.{table_name}'

        for c in update_data.columns:
            type = str(update_data[c].dtypes)
            if type == 'object':
                update_data[c] = update_data[c].astype("string")
            elif type == 'datetime64[ns, UTC]':
                update_data[c] = update_data[c].dt.strftime(config.DWH_TIME_FORMAT)
                update_data[c] = pd.to_datetime(update_data[c], errors='coerce', format="%Y-%m-%d %H:%M:%S")
        try:
            print(f"Load data to {table_id}...")
            pandas_gbq.to_gbq(update_data, destination_table=table_id, chunksize=50000, if_exists=if_exists,
                              credentials=self.credentials, api_method="load_csv")
        except Exception as e:
            print(e)
            raise e

    def bq_truncate_and_append(self, data_truncate, dataset_id, table_name, project_id=config.PROJECT_ID):
        target_table_schema = self.get_table(table_id=f"{project_id}.{dataset_id}.{table_name}").schema
        target_schema = [{'name': field.name, 'type': field.field_type, 'mode': field.mode} for field in
                         target_table_schema]
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=target_schema
        )
        table_ref = self.client.dataset(dataset_id).table(table_name)

        for col_name in [col['name'] for col in target_schema]:
            if col_name not in data_truncate.columns:
                data_truncate[col_name] = None
        job = self.client.load_table_from_dataframe(
            data_truncate, table_ref, job_config=job_config
        )
        job.result()

    def check_table_exists(self, table_ref):
        try:
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False
    def generate_create_table_sql(self, project_id, dataset_id, table_name, schema_dict):
        # Tạo lệnh SQL từ dictionary schema
        columns = []
        for col, col_type in schema_dict.items():
            columns.append(f"{col} {col_type}")
        columns_sql = ",\n  ".join(columns)
        sql = f"""
        CREATE TABLE `{project_id}.{dataset_id}.{table_name}` (
                account_currency STRING,
  account_id STRING,
  account_name STRING,
  action_values STRING,
  actions STRING,
  ad_click_actions STRING,
  ad_id STRING,
  ad_impression_actions STRING,
  ad_name STRING,
  adset_id STRING,
  adset_name STRING,
  attribution_setting STRING,
  auction_bid STRING,
  auction_competitiveness STRING,
  auction_max_competitor_bid STRING,
  buying_type STRING,
  campaign_id STRING,
  campaign_name STRING,
  canvas_avg_view_percent STRING,
  canvas_avg_view_time STRING,
  catalog_segment_actions STRING,
  catalog_segment_value STRING,
  catalog_segment_value_mobile_purchase_roas STRING,
  catalog_segment_value_omni_purchase_roas STRING,
  catalog_segment_value_website_purchase_roas STRING,
  clicks STRING,
  conversion_values STRING,
  conversions STRING,
  converted_product_quantity STRING,
  converted_product_value STRING,
  cost_per_15_sec_video_view STRING,
  cost_per_2_sec_continuous_video_view STRING,
  cost_per_action_type STRING,
  cost_per_ad_click STRING,
  cost_per_conversion STRING,
  cost_per_dda_countby_convs STRING,
  cost_per_inline_link_click STRING,
  cost_per_inline_post_engagement STRING,
  cost_per_one_thousand_ad_impression STRING,
  cost_per_outbound_click STRING,
  cost_per_thruplay STRING,
  cost_per_unique_action_type STRING,
  cost_per_unique_click STRING,
  cost_per_unique_conversion STRING,
  cost_per_unique_inline_link_click STRING,
  cost_per_unique_outbound_click STRING,
  cpc STRING,
  cpm STRING,
  cpp STRING,
  created_time DATETIME,
  ctr STRING,
  date_start DATETIME,
  date_stop DATETIME,
  dda_countby_convs STRING,
  dda_results STRING,
  frequency STRING,
  full_view_impressions STRING,
  full_view_reach STRING,
  impressions STRING,
  inline_link_click_ctr STRING,
  inline_link_clicks STRING,
  inline_post_engagement STRING,
  instagram_upcoming_event_reminders_set STRING,
  instant_experience_clicks_to_open STRING,
  instant_experience_clicks_to_start STRING,
  instant_experience_outbound_clicks STRING,
  interactive_component_tap STRING,
  marketing_messages_delivery_rate STRING,
  mobile_app_purchase_roas STRING,
  objective STRING,
  optimization_goal STRING,
  outbound_clicks STRING,
  outbound_clicks_ctr STRING,
  place_page_name STRING,
  purchase_roas STRING,
  qualifying_question_qualify_answer_rate STRING,
  reach STRING,
  social_spend STRING,
  spend STRING,
  updated_time STRING,
  video_30_sec_watched_actions STRING,
  video_avg_time_watched_actions STRING,
  video_continuous_2_sec_watched_actions STRING,
  video_p100_watched_actions STRING,
  video_p25_watched_actions STRING,
  video_p50_watched_actions STRING,
  video_p75_watched_actions STRING,
  video_p95_watched_actions STRING,
  video_play_actions STRING,
  video_play_curve_actions STRING,
  video_play_retention_0_to_15s_actions STRING,
  video_play_retention_20_to_60s_actions STRING,
  video_play_retention_graph_actions STRING,
  video_time_watched_actions STRING,
  website_ctr STRING,
  website_purchase_roas STRING
        )
        PARTITION BY DATE_TRUNC(date_start, MONTH)
        """
        return sql

    def bq_upsert(self, data_upsert, dataset_id, table_name, primary_keys=[], except_columns=[],
              project_id=config.PROJECT_ID):
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        print("=" * 80)
        print(f"[BQ_UPSERT] 🔹 Bắt đầu upsert dữ liệu vào bảng: {table_id}")
        print(f"[BQ_UPSERT] 🔹 Số dòng DataFrame đầu vào: {len(data_upsert)}")
        print(f"[BQ_UPSERT] 🔹 Số cột DataFrame đầu vào: {len(data_upsert.columns)}")
        print(f"[BQ_UPSERT] 🔹 Primary keys: {primary_keys}")
        print(f"[BQ_UPSERT] 🔹 Except columns: {except_columns}")

        # ✅ Kiểm tra bảng target
        try:
            target_table = self.get_table(table_id)
            target_table_schema = target_table.schema
            print(f"[BQ_UPSERT] ✅ Bảng {table_id} đã tồn tại. Số cột trong schema: {len(target_table_schema)}")
        except Exception as e:
            print(f"[BQ_UPSERT] ⚠️ Bảng {table_id} không tồn tại. Đang tạo bảng mới.")
            target_table_schema = [bigquery.SchemaField(col, TBL_FB_AD_CAMPAIGN_METRICS_RECORD1[col]) for col in TBL_FB_AD_CAMPAIGN_METRICS_RECORD1.keys()]
            create_sql = self.generate_create_table_sql(project_id, dataset_id, table_name, TBL_FB_AD_CAMPAIGN_METRICS_RECORD1)
            print(f"[BQ_UPSERT] 🧱 SQL tạo bảng:\n{create_sql}")
            self.execute(create_sql)
            target_table = self.get_table(table_id)

        target_schema = [{'name': field.name, 'type': field.field_type, 'mode': field.mode} for field in
                        target_table_schema if (field.name not in except_columns) and (field.name not in primary_keys)]

        print(f"[BQ_UPSERT] 🔸 Số cột trong target schema sau lọc: {len(target_schema)}")

        # ✅ Tạo bảng tạm
        temp_table_id = f'{project_id}.temp1.{table_name}'
        temp_table_ref = self.client.dataset('temp1').table(table_name)
        temp_table = bigquery.Table(temp_table_ref, schema=target_table_schema)
        if not self.check_table_exists(temp_table_ref):
            print(f"[BQ_UPSERT] 🧩 Bảng tạm {temp_table_id} chưa tồn tại — tạo mới.")
            self.client.create_table(temp_table, exists_ok=True)
        else:
            print(f"[BQ_UPSERT] ♻️ Bảng tạm {temp_table_id} đã tồn tại — sẽ ghi đè (TRUNCATE).")

        # ✅ Cấu hình ghi dữ liệu
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=target_schema
        )

        # ✅ Đảm bảo đầy đủ các cột
        for col_name in [col['name'] for col in target_schema]:
            if col_name not in data_upsert.columns:
                print(f"[BQ_UPSERT] ➕ Thêm cột thiếu: {col_name}")
                data_upsert[col_name] = None
                if col_name not in except_columns:
                    except_columns.append(col_name)

        # ✅ Ép kiểu dữ liệu phù hợp với schema
        datetime_cols = [f['name'] for f in target_schema if f['type'] in ["DATETIME", "TIMESTAMP"]]
        numeric_cols = [f['name'] for f in target_schema if f['type'] in ["INTEGER", "FLOAT", "NUMERIC", "BIGNUMERIC"]]
        string_cols = [f['name'] for f in target_schema if f['type'] == "STRING"]

        print(f"[BQ_UPSERT] 🔹 Ép kiểu dữ liệu: {len(datetime_cols)} datetime, {len(numeric_cols)} numeric, {len(string_cols)} string columns.")

        for col in data_upsert.columns:
            if col in datetime_cols:
                data_upsert[col] = pd.to_datetime(data_upsert[col], errors='coerce')
            elif col in numeric_cols:
                data_upsert[col] = pd.to_numeric(data_upsert[col], errors='coerce')
            elif col in string_cols:
                data_upsert[col] = data_upsert[col].astype(str).replace("nan", None)

        print(f"[BQ_UPSERT] ✅ Dữ liệu đã ép kiểu — bắt đầu load lên bảng tạm...")

        # ✅ Ghi dữ liệu vào bảng tạm
        load_job = self.client.load_table_from_dataframe(data_upsert, temp_table_id, job_config=job_config)
        load_result = load_job.result()
        print(f"[BQ_UPSERT] 📤 Đã upload dữ liệu lên bảng tạm {temp_table_id}")
        print(f"[BQ_UPSERT]    • Output rows: {load_result.output_rows}")
        print(f"[BQ_UPSERT]    • Job ID: {load_result.job_id}")

        # ✅ Kiểm tra số dòng trong bảng tạm
        temp_count_df = self.client.query(f"SELECT COUNT(*) as c FROM `{temp_table_id}`").result().to_dataframe()
        print(f"[BQ_UPSERT] 🔍 Số dòng trong bảng tạm sau khi load: {temp_count_df['c'].iloc[0]}")

        # ✅ Tạo câu lệnh MERGE
        merge_query = f"""
            MERGE INTO `{project_id}.{dataset_id}.{table_name}` target
            USING `{temp_table_id}` source
            ON {' AND '.join(f'target.{key} = source.{key}' for key in primary_keys)}
            WHEN MATCHED THEN
                UPDATE SET {', '.join(f"target.{col} = source.{col}" for col in data_upsert.columns if col not in except_columns)}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(data_upsert.columns)})
                VALUES ({', '.join(f"source.{col}" for col in data_upsert.columns)})
        """

        print("[BQ_UPSERT] 🧠 SQL MERGE Query:")
        print(merge_query)

        # ✅ Thực thi MERGE
        merge_result = self.execute(merge_query)
        print(f"[BQ_UPSERT] ✅ MERGE hoàn tất.")

        # ✅ Kiểm tra kết quả sau MERGE
        target_count_df = self.client.query(f"SELECT COUNT(*) as c FROM `{project_id}.{dataset_id}.{table_name}`").result().to_dataframe()
        print(f"[BQ_UPSERT] 📊 Số dòng trong bảng {table_id} sau MERGE: {target_count_df['c'].iloc[0]}")

        # ✅ Xóa bảng tạm
        if self.check_table_exists(temp_table_ref):
            print(f"[BQ_UPSERT] 🗑️ Xóa bảng tạm {temp_table_id}")
            self.client.delete_table(temp_table, not_found_ok=True)

        print(f"[BQ_UPSERT] 🎯 Upsert hoàn tất cho bảng {table_id}")
        print("=" * 80)


