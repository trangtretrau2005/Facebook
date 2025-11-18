from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import lit, to_timestamp
from google.cloud import storage,bigquery
from datetime import datetime
from logging import Logger
from google.oauth2 import service_account
import time
from config import config
# # Cần một biến config.SERVICE_ACCOUNT để GCSHelper hoạt động nếu không truyền credentials_file
# # Giả định SERVICE_ACCOUNT là đường dẫn đến key JSON.
# # Nếu không có, bạn có thể thay thế bằng self.sa_key_path.
from plugins.helper.gcp_helper import BQHelper
TBL_FB_AD_CAMPAIGN_METRICS_RECORD = {
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
    "created_time": "datetime64[ns]",
    "ctr": "string",
    "date_start": "datetime64[ns]",
    "date_stop": "datetime64[ns]",
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

# ================================
# GCS HELPER
# ================================
class GCSHelper:

    def __init__(self, logger: Logger, client=None, credentials_file=None, bucket_name=None):
        self.logger = logger
        self.bucket_name = bucket_name

        # 1. Tải credentials
        credentials_path = credentials_file or config.SERVICE_ACCOUNT
        try:
            self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
        except Exception as e:
            self.logger.error(f"❌ Lỗi tải credentials từ {credentials_path}: {e}")
            raise

        # 2. Khởi tạo Client và Bucket
        self.client = client if client else storage.Client(credentials=self.credentials)
        if self.bucket_name:
            try:
                self.bucket = self.client.get_bucket(self.bucket_name)
            except Exception as e:
                self.logger.error(f"❌ Lỗi truy cập bucket {self.bucket_name}: {e}")
                self.bucket = None
        else:
            self.bucket = None

    def get_blob_from_path(self, gcs_path: str):
        """Phân tích đường dẫn gs:// và trả về blob object."""
        if not gcs_path.startswith("gs://"):
            self.logger.error(f"Đường dẫn GCS không hợp lệ: {gcs_path}")
            return None

        path_parts = gcs_path[5:].split("/", 1)
        bucket_name = path_parts[0]
        blob_name = path_parts[1] if len(path_parts) > 1 else ""

        if self.bucket and self.bucket.name == bucket_name:
            bucket_ref = self.bucket
        else:
            try:
                bucket_ref = self.client.get_bucket(bucket_name)
            except Exception as e:
                self.logger.error(f"❌ Lỗi truy cập bucket {bucket_name}: {e}")
                return None

        return bucket_ref.blob(blob_name)

    def download_json(self, blob: storage.Blob):
        try:
            json_string = blob.download_as_text(encoding='utf-8')
            self.logger.info(f"JSON file downloaded from GCS: gs://{blob.bucket.name}/{blob.name}")
            return json_string
        except Exception as e:
            # Nếu blob là None, tránh attribute access error
            blob_id = getattr(blob, "name", "unknown")
            self.logger.error(f"❌ Error when downloading from GCS://{blob_id}: {e}")
            return None


# ================================
# FACEBOOK SPARK TRANSFORM (Cải tiến logic BQ)
# ================================
class FacebookSparkTransform:
    def __init__(
        self,
        logger: Logger,
        gcs_folders: list,
        bq_output_table: str,        # có thể "project.dataset.table" hoặc "dataset.table"
        service_account_key_path: str,
        spark_master: str = "local[*]"
    ):
        self.logger = logger
        self.gcs_folders = gcs_folders
        self.bq_output_table = bq_output_table
        self.sa_key_path = service_account_key_path
        self.spark_master = spark_master
        self.schema = self._build_schema()

    # ==========================================
    # BUILD SPARK SCHEMA (Không thay đổi)
    # ==========================================
    def _build_schema(self):
        fields = []
        for col, typ in TBL_FB_AD_CAMPAIGN_METRICS_RECORD.items():
            fields.append(StructField(col, StringType(), True))
        return StructType(fields)

    # ==========================================
    # EXTRACTION: Get latest JSON from each GCS folder
    # ==========================================
    def _extract_latest_json_from_gcs(self):
        client = storage.Client.from_service_account_json(self.sa_key_path)
        json_list = []

        for gcs_folder in self.gcs_folders:
            if not gcs_folder.startswith("gs://"):
                raise ValueError(f"❌ gcs_folder '{gcs_folder}' không đúng format 'gs://bucket/prefix'")
            clean_path = gcs_folder[5:]
            parts = clean_path.split("/", 1)
            if len(parts) != 2:
                raise ValueError(f"❌ gcs_folder '{gcs_folder}' thiếu prefix. Format đúng: gs://bucket/folder/")
            bucket_name, prefix = parts
            bucket = client.bucket(bucket_name)
            blobs = list(bucket.list_blobs(prefix=prefix))
            if not blobs:
                self.logger.warning(f"⚠ Không tìm thấy file trong folder: {gcs_folder}")
                continue
            latest_blob = max(blobs, key=lambda b: b.updated)
            self.logger.info(f"📥 Lấy file mới nhất từ {gcs_folder}: {latest_blob.name}")
            json_string = latest_blob.download_as_text()
            json_list.append(json_string)

        return json_list

    # ==========================================
    # CREATE SPARK SESSION
    # ==========================================
    def _create_spark(self):
        BIGQUERY_CONNECTOR_PACKAGE = "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1"
        packages = [BIGQUERY_CONNECTOR_PACKAGE]

        spark_builder = (
            SparkSession.builder
            .appName("Facebook Spark Transform (Local BQ)")
            .master(self.spark_master)
            .config("spark.jars.packages", ",".join(packages))
            .config("spark.bigquery.project", getattr(config, "PROJECT_ID", "changchang-476607"))
            .config("spark.google.cloud.projectId", getattr(config, "PROJECT_ID", "changchang-476607"))
            .config("credentialsFile", self.sa_key_path)
            .getOrCreate()
        )
        return spark_builder

    # ==========================================
    # Parse table id
    # ==========================================
    def _parse_table_id(self, table_id_str: str):
        """
        Trả về tuple (project, dataset, table)
        Hỗ trợ 'project.dataset.table' hoặc 'dataset.table'
        """
        parts = table_id_str.split(".")
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            project = getattr(config, "PROJECT_ID", None)
            if not project:
                raise ValueError("PROJECT_ID không được cấu hình trong config và bq_output_table chỉ có dạng dataset.table")
            return project, parts[0], parts[1]
        else:
            raise ValueError(f"Định dạng bq_output_table không hợp lệ: {table_id_str}")

    # ==========================================
    # RUN - Main flow
    # ==========================================
    def run(self, **kwargs):
        self.logger.info("🚀 BẮT ĐẦU SPARK TRANSFORM FACEBOOK (LOCAL / IN-MEMORY)")

        # 1) Extract JSON list
        self.logger.info("1️⃣ 📥 BẮT ĐẦU EXTRACTION (LẤY FILE MỚI NHẤT TỪ MỖI FOLDER)...")
        json_list = self._extract_latest_json_from_gcs()
        if not json_list:
            self.logger.error("❌ Không có dữ liệu JSON để xử lý.")
            return
        self.logger.info(f"📌 Tìm thấy {len(json_list)} file JSON mới nhất từ các folder.")

        # 2) Create Spark
        spark = self._create_spark()
        self.logger.info("2️⃣ 🧩 SPARK SESSION READY")

        # 3) Convert list -> RDD of lines (each jsonl line)
        self.logger.info("3️⃣ 🔄 Tạo RDD từ danh sách JSON...")
        rdd_files = spark.sparkContext.parallelize(json_list, len(json_list))
        lines_rdd = rdd_files.flatMap(lambda file_str: file_str.strip().split("\n"))

        # 4) Read JSON into DataFrame using provided schema
        self.logger.info("4️⃣ 📄 Đọc JSON thành DataFrame...")
        try:
            df = spark.read.schema(self.schema).json(lines_rdd)
        except Exception as e:
            self.logger.error(f"❌ Lỗi đọc JSON → DataFrame: {e}")
            spark.stop()
            return

        # 5) Transform
        self.logger.info("5️⃣ 🛠️ Bắt đầu Transform (chuẩn hoá + xoá duplicate)...")
        df = (
            df.withColumn("date_start", to_timestamp("date_start"))
              .withColumn("date_stop", to_timestamp("date_stop"))
              .withColumn("created_time", to_timestamp("created_time"))
              .withColumn("updated_time", to_timestamp("updated_time"))
              .withColumn("extraction_timestamp", lit(datetime.now()))
        )
        df = df.dropDuplicates(["campaign_id", "date_start", "date_stop"])

        # 6) PREPARE BigQuery client and table ids
        self.logger.info("6️⃣ 🔌 Chuẩn bị BigQuery client & table names")
        project_id, dataset_id, table_name = self._parse_table_id(self.bq_output_table)
        target_table_id = f"{project_id}.{dataset_id}.{table_name}"
        # Tạo bảng tạm có tên duy nhất để tránh xung đột
        ts = int(time.time())
        tmp_table_name = f"{config.tmp_table_name}_{ts}"
        tmp_table_id = f"{project_id}.{dataset_id}.{tmp_table_name}"

        # Tạo Credentials và client BigQuery
        creds = service_account.Credentials.from_service_account_file(self.sa_key_path)
        bq_client = bigquery.Client(project=project_id, credentials=creds)

        # 7) Viết DataFrame vào bảng tạm trên BigQuery (overwrite nếu đã tồn tại - an toàn)
        self.logger.info(f"7️⃣ ⤓ Ghi dataframe vào bảng tạm:")
        try:
            (
            df.write.format("bigquery")
            .option("project", project_id)
            .option("parentProject", project_id)
            .option("dataset", dataset_id)
            .option("table", tmp_table_name)
            .option("credentialsFile", self.sa_key_path)

            # ⭐ Không ghi file tạm lên GCS
            .option("writeMethod", "direct")

            .mode("overwrite")
            .save()
        )
        except Exception as e:
            self.logger.error(f"❌ Lỗi khi ghi bảng tạm {tmp_table_id}: {e}")
            spark.stop()
            return

        # 8) Kiểm tra bảng chính có tồn tại không
        self.logger.info(f"8️⃣ 🔎 Kiểm tra bảng chính: {target_table_id}")
        target_exists = True
        try:
            bq_client.get_table(target_table_id)  # gọi sẽ raise nếu không tồn tại
        except Exception:
            target_exists = False

        # 9) Nếu bảng chính chưa tồn tại -> tạo bảng chính từ tmp (CREATE TABLE AS SELECT)
        if not target_exists:
            self.logger.warning(f"⚠️ Bảng chính {target_table_id} chưa tồn tại. Tạo bảng mới từ bảng tạm {tmp_table_id}...")
            try:
                create_sql = f"CREATE TABLE `{target_table_id}` AS SELECT * FROM `{tmp_table_id}`"
                query_job = bq_client.query(create_sql)
                query_job.result()
                self.logger.info(f"✅ Tạo bảng chính {target_table_id} thành công.")
            except Exception as e:
                self.logger.error(f"❌ Lỗi khi tạo bảng chính từ tmp: {e}")
                # Xóa tmp trước khi return nếu muốn
                try:
                    bq_client.delete_table(tmp_table_id, not_found_ok=True)
                except Exception:
                    pass
                spark.stop()
                return
        else:
            # 10) Nếu bảng chính tồn tại -> MERGE từ tmp vào bảng chính
            self.logger.info(f"9️⃣ 🔁 Bảng chính tồn tại. Thực hiện MERGE từ {tmp_table_id} vào {target_table_id} ...")

            # primary keys
            primary_keys = ["campaign_id", "date_start", "date_stop"]
            # đảm bảo các primary keys tồn tại trong dataframe (nếu không, sẽ raise)
            all_columns = df.columns
            for pk in primary_keys:
                if pk not in all_columns:
                    self.logger.error(f"❌ Primary key '{pk}' không tồn tại trong dataframe columns.")
                    # cleanup tmp rồi return
                    bq_client.delete_table(tmp_table_id, not_found_ok=True)
                    spark.stop()
                    return

            except_columns = primary_keys + ["extraction_timestamp"]
            update_columns = [col for col in all_columns if col not in except_columns]

            # Build MERGE statement
            on_conditions = " AND ".join([f"target.{k} = source.{k}" for k in primary_keys])

            update_set = ", ".join([f"target.{c} = source.{c}" for c in update_columns]) if update_columns else ""

            insert_cols = ", ".join([f"`{c}`" for c in all_columns])
            insert_vals = ", ".join([f"source.`{c}`" for c in all_columns])

            merge_sql = f"""
                MERGE `{target_table_id}` AS target
                USING `{tmp_table_id}` AS source
                ON {on_conditions}
                WHEN MATCHED THEN
                    {f"UPDATE SET {update_set}" if update_set else "-- no update columns"}
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            try:
                temp_count_df = bq_client.query(
                    f"SELECT COUNT(*) AS c FROM `{tmp_table_id}`"
                ).result().to_dataframe()

                temp_cnt = int(temp_count_df['c'].iloc[0]) if not temp_count_df.empty else 0
                self.logger.info(f"[BQ_UPSERT] 🔍 Rows in TMP table before MERGE: {temp_cnt}")
            except Exception as e:
                self.logger.warning(f"[BQ_UPSERT] ⚠️ Không thể đếm dòng bảng tạm: {e}")

            try:
                query_job = bq_client.query(merge_sql)
                query_job.result()
                self.logger.info("✅ MERGE completed successfully.")
            except Exception as e:
                self.logger.error(f"❌ Lỗi khi chạy MERGE: {e}")
                try:
                    bq_client.delete_table(tmp_table_id, not_found_ok=True)
                except Exception:
                    pass
                spark.stop()
                return

            # ==========================================================
            # ✅ 2) ĐẾM SỐ DÒNG TRONG BẢNG CHÍNH SAU MERGE
            # ==========================================================
            try:
                target_count_df = bq_client.query(
                    f"SELECT COUNT(*) AS c FROM `{target_table_id}`"
                ).result().to_dataframe()

                target_cnt = int(target_count_df['c'].iloc[0]) if not target_count_df.empty else 0
                self.logger.info(f"[BQ_UPSERT] 📊 Rows in TARGET table after MERGE: {target_cnt}")
            except Exception as e:
                self.logger.warning(f"[BQ_UPSERT] ⚠️ Không thể đếm dòng bảng chính sau MERGE: {e}")