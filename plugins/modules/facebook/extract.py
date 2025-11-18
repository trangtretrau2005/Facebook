import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging import Logger

import pandas as pd
from airflow.models import Variable
from airflow.utils.context import Context

from plugins.helper import time_helper
from plugins.helper.facebook_helper import FacebookHelper
from plugins.helper.gcp_helper import GGSheetHelper, BQHelper, GCSHelper


class ExtractFacebookAdCampaignMetrics:
    def __init__(self,
                 logger: Logger,
                 api_client: FacebookHelper,
                 gg_sheet_client: GGSheetHelper,
                 table_name: str,
                 gcs: GCSHelper,
                 execution_date: str,
                 kwargs: Context):
        self.logger = logger
        self.facebook = api_client
        self.gcs = gcs
        self.google = gg_sheet_client
        self.table_name = table_name
        self.execution_date = execution_date
        self.kwargs = kwargs

    def prepare(self):
        sheet_id = self.kwargs.get('sheet_id', 'unknown')
        sheet_name = self.kwargs.get('sheet_name', 'Facebook')
        config_details = self.google.get_sheet_data(sheet_id=sheet_id, sheet_name=sheet_name)

        return config_details

    def parse(self, results):
        records = []
        for result in results:
            for item in result.get('insights', {}).get('data', []):
                record = {}
                for field in FacebookHelper.list_field_ad_set_insights:
                    record[field] = item.get(field, None)
                records.append(record)

        return records

    def extract(self, bm_config, start_info, end):

        system_user_token = bm_config[1]
        params = {'access_token': system_user_token}
        results = []

        try:
            # Step 1: Kiểm tra token
            response = self.facebook.get_data(path_id="me", params=params)

            system_user_id = None
            if isinstance(response, list) and len(response) > 0:
                system_user_id = response[0].get('id')

            if not system_user_id:
                return []
            response = self.facebook.get_data(
                path_id="me?fields=adaccounts",
                params=params,
                data_key='adaccounts',
                paging_key='adaccounts'
            )

            ad_account_ids = [
                item.get('id')
                for ad_account in response
                for item in ad_account.get('data', [])
            ]

            if not ad_account_ids:
                return []

            # Step 4: Lấy campaigns
            params['fields'] = 'insights.date_preset(maximum){' + \
                f"{','.join(FacebookHelper.list_field_ad_campaign_insights)}" + '}'
            params['limit'] = 300


            with ThreadPoolExecutor(max_workers=len(ad_account_ids)) as api_executor:
                futures = [
                    api_executor.submit(
                        self.facebook.get_data,
                        f"{ad_account_id}/campaigns",
                        params,
                        data_key='data'
                    )
                    for ad_account_id in ad_account_ids
                ]

                for i, future in enumerate(as_completed(futures)):
                    try:
                        response = future.result()
                        if not response:
                            print(f"⚠️ [THREAD {i}] Không có data trả về từ ad_account.")
                        else:
                            print(f"✅ [THREAD {i}] Nhận {len(response)} items.")
                            results.extend(response)
                    except Exception:
                        print(f"❌ [THREAD {i}] Lỗi khi gọi API campaign:")

            print(f"✅ [EXTRACT] Hoàn tất, tổng số result thu được: {len(results)}")
            return results

        except Exception:
            return []
    def save_to_gcs(self, df, system_user_id):
        """
        Save extracted/parsed data to GCS as JSONL
        """
        jsonl_data = df.to_json(orient="records", force_ascii=False, lines=True)

        file_path = f"facebook2/{self.execution_date}/{system_user_id}.jsonl"
        self.logger.info(f"[{system_user_id}] Uploading extract to GCS → {file_path}")

        self.gcs.upload_json(
            json_string=jsonl_data,
            file_name=file_path
        )

        return file_path
    
    def run_task(self, bm_config, FACEBOOK_SOURCES, lock):
        """
        Ingest for single BM.
        """
        system_user_id = bm_config[0]
        end = int(time_helper.get_now().timestamp())

        # extract
        self.logger.info(f"[{system_user_id}] Extract {self.table_name}...")
        results = self.extract(
            bm_config=bm_config,
            start_info=FACEBOOK_SOURCES,
            end=end
        )

        if len(results) == 0:
            self.logger.info(f"[{system_user_id}] Empty data → tạo record EMPTY để lưu vào BQ.")

            empty_record = {
                "campaign_id": None,
                "date_start": self.execution_date,
                "date_stop": self.execution_date,
                "bm_id": bm_config[0],
                "ad_account_id": None,
                "data_status": "EMPTY"
            }

            df = pd.DataFrame([empty_record])

            # Lưu GCS (tùy bạn có muốn hay không)
            self.save_to_gcs(df, system_user_id)
        else:
            self.logger.info(f"[{system_user_id}] Extract {self.table_name} is done: {len(results)} items!")

        parsed_records = self.parse(results)

        df = pd.DataFrame(parsed_records)
        
        self.save_to_gcs(df, system_user_id)

    def run(self, run=True):
        """
        Ingest Facebook Pages data from Facebook Graph API.
        """
        if not run:
            self.logger.info(f"Skip run ingestion for {self.table_name}.")

        FACEBOOK_PAGE_SOURCES = Variable.get('FACEBOOK_PAGE_SOURCES', default_var={}, deserialize_json=True)

        bm_configs = self.prepare()

        lock = threading.Lock()
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self.run_task, bm_config, FACEBOOK_PAGE_SOURCES, lock) for idx, bm_config
                in enumerate(bm_configs)
            ]
            for future in as_completed(futures):
                future.result()

        Variable.set("FACEBOOK_PAGE_SOURCES", FACEBOOK_PAGE_SOURCES, serialize_json=True)
