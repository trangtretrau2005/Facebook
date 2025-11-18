import re
import time
from logging import Logger

import requests
from requests.exceptions import HTTPError, RequestException

from config import config



class FacebookHelper:
    session = None
    list_fields_ad_account = ["account_id", "account_status", "age", "agency_client_declaration", "amount_spent",
                              "attribution_spec", "balance", "business", "business_city", "business_country_code",
                              "business_name", "business_state", "business_street", "business_street2", "business_zip",
                              "can_create_brand_lift_study", "capabilities", "created_time", "currency",
                              "custom_audience_info", "default_dsa_beneficiary", "default_dsa_payor", "disable_reason",
                              "end_advertiser", "end_advertiser_name", "existing_customers",
                              "extended_credit_invoice_group", "failed_delivery_checks", "fb_entity", "funding_source",
                              "has_migrated_permissions", "id", "io_number", "is_attribution_spec_system_default",
                              "is_direct_deals_enabled", "is_in_3ds_authorization_enabled_market",
                              "is_notifications_enabled", "is_personal", "is_prepay_account", "is_tax_id_required",
                              "line_numbers", "media_agency", "min_campaign_group_spend_cap", "min_daily_budget",
                              "name",
                              "offsite_pixels_tos_accepted", "owner", "partner", "rf_spec", "spend_cap", "tax_id",
                              "tax_id_status", "tax_id_type", "timezone_id", "timezone_name",
                              "timezone_offset_hours_utc",
                              "tos_accepted", "user_tasks", "user_tos_accepted"]
    list_fields_campaigns = ['id', 'account_id', 'adlabels', 'bid_strategy', 'boosted_object_id', 'brand_lift_studies',
                             'budget_rebalance_flag', 'budget_remaining', 'buying_type', 'campaign_group_active_time',
                             'can_create_brand_lift_study', 'can_use_spend_cap', 'configured_status', 'created_time',
                             'daily_budget', 'effective_status', 'has_secondary_skadnetwork_reporting',
                             'is_budget_schedule_enabled', 'is_skadnetwork_attribution', 'issues_info',
                             'last_budget_toggling_time', 'lifetime_budget', 'name', 'objective', 'pacing_type',
                             'primary_attribution', 'promoted_object', 'smart_promotion_type', 'source_campaign',
                             'source_campaign_id', 'special_ad_categories', 'special_ad_category',
                             'special_ad_category_country', 'spend_cap', 'start_time', 'status', 'stop_time',
                             'topline_id', 'updated_time']
    list_fields_adsets = ['id', 'account_id', 'adlabels', 'adset_schedule', 'asset_feed_id', 'attribution_spec',
                          'bid_adjustments', 'bid_amount', 'bid_constraints', 'bid_info', 'bid_strategy',
                          'billing_event', 'brand_safety_config', 'budget_remaining', 'campaign',
                          'campaign_active_time', 'campaign_attribution', 'campaign_id', 'configured_status',
                          'created_time', 'creative_sequence', 'daily_budget', 'daily_min_spend_target',
                          'daily_spend_cap', 'destination_type', 'dsa_beneficiary', 'dsa_payor', 'effective_status',
                          'end_time', 'frequency_control_specs', 'instagram_actor_id', 'is_dynamic_creative',
                          'issues_info', 'learning_stage_info', 'lifetime_budget', 'lifetime_imps',
                          'lifetime_min_spend_target', 'lifetime_spend_cap', 'min_budget_spend_percentage',
                          'multi_optimization_goal_weight', 'name', 'optimization_goal', 'optimization_sub_event',
                          'pacing_type', 'promoted_object', 'recommendations', 'recurring_budget_semantics',
                          'regional_regulated_categories', 'regional_regulation_identities', 'review_feedback',
                          'rf_prediction_id', 'source_adset', 'source_adset_id', 'start_time', 'status', 'targeting',
                          'targeting_optimization_types', 'time_based_ad_rotation_id_blocks',
                          'time_based_ad_rotation_intervals', 'updated_time', 'use_new_app_click']
    list_fields_ads = ['account_id', 'ad_active_time', 'ad_review_feedback', 'adlabels', 'adset', 'adset_id',
                       'bid_amount',
                       'campaign', 'campaign_id',
                       'configured_status', 'conversion_domain', 'created_time', 'creative', 'effective_status', 'id',
                       'issues_info', 'last_updated_by_app_id', 'name', 'preview_shareable_link', 'recommendations',
                       'source_ad', 'source_ad_id', 'status', 'tracking_specs', 'updated_time']
    list_field_page_insights = ['page_total_actions', 'page_post_engagements', 'page_fan_adds_by_paid_non_paid_unique',
                                'page_lifetime_engaged_followers_unique', 'page_daily_follows',
                                'page_daily_follows_unique',
                                'page_daily_unfollows_unique', 'page_follows', 'page_impressions',
                                'page_impressions_unique',
                                'page_impressions_paid', 'page_impressions_paid_unique', 'page_impressions_viral', '',
                                'page_posts_impressions_unique', 'page_posts_impressions_paid',
                                'page_posts_impressions_paid_unique', 'page_posts_impressions_organic_unique',
                                'page_posts_served_impressions_organic_unique', 'page_posts_impressions_viral',
                                'page_posts_impressions_viral_unique', 'page_posts_impressions_nonviral',
                                'page_posts_impressions_nonviral_unique', 'post_clicks', 'post_clicks_by_type',
                                'post_impressions', 'post_impressions_unique', 'post_impressions_paid',
                                'post_impressions_paid_unique', 'post_impressions_fan', 'post_impressions_fan_unique',
                                'post_impressions_organic', 'post_impressions_organic_unique', 'post_impressions_viral',
                                'post_impressions_viral_unique', 'post_impressions_nonviral',
                                'post_impressions_nonviral_unique', 'post_reactions_like_total',
                                'post_reactions_love_total',
                                'post_reactions_wow_total', 'post_reactions_haha_total', 'post_reactions_sorry_total',
                                'post_reactions_anger_total', 'post_reactions_by_type_total',
                                'page_actions_post_reactions_like_total', 'page_actions_post_reactions_love_total',
                                'page_actions_post_reactions_wow_total', 'page_actions_post_reactions_haha_total',
                                'page_actions_post_reactions_sorry_total', 'page_actions_post_reactions_anger_total',
                                'page_actions_post_reactions_total', 'page_fans', 'page_fans_locale', 'page_fans_city',
                                'page_fans_country', 'page_fan_adds', 'page_fan_adds_unique', 'page_fan_removes',
                                'page_fan_removes_unique', 'page_video_views', 'page_video_views_by_uploaded_hosted',
                                'page_video_views_paid', 'page_video_views_organic',
                                'page_video_views_by_paid_non_paid',
                                'page_video_views_autoplayed', 'page_video_views_click_to_play',
                                'page_video_views_unique',
                                'page_video_repeat_views', 'page_video_complete_views_30s',
                                'page_video_complete_views_30s_paid', 'page_video_complete_views_30s_organic',
                                'page_video_complete_views_30s_autoplayed',
                                'page_video_complete_views_30s_click_to_play',
                                'page_video_complete_views_30s_unique', 'page_video_complete_views_30s_repeat_views',
                                'post_video_complete_views_30s_autoplayed',
                                'post_video_complete_views_30s_clicked_to_play',
                                'post_video_complete_views_30s_organic', 'post_video_complete_views_30s_paid',
                                'post_video_complete_views_30s_unique', 'page_video_view_time', 'page_views_total',
                                'post_video_avg_time_watched', 'post_video_complete_views_organic',
                                'post_video_complete_views_organic_unique', 'post_video_complete_views_paid',
                                'post_video_complete_views_paid_unique', 'post_video_retention_graph',
                                'post_video_retention_graph_clicked_to_play', 'post_video_retention_graph_autoplayed',
                                'post_video_views_organic', 'post_video_views_organic_unique', 'post_video_views_paid',
                                'post_video_views_paid_unique', 'post_video_length', 'post_video_views',
                                'post_video_views_unique', 'post_video_views_autoplayed',
                                'post_video_views_clicked_to_play',
                                'post_video_views_15s', 'post_video_views_60s_excludes_shorter',
                                'post_video_views_sound_on',
                                'post_video_view_time', 'post_video_view_time_organic',
                                'post_video_view_time_by_age_bucket_and_gender', 'post_video_view_time_by_region_id',
                                'post_video_views_by_distribution_type', 'post_video_view_time_by_distribution_type',
                                'post_video_view_time_by_country_id', 'post_video_views_live',
                                'post_video_social_actions_count_unique', 'post_activity_by_action_type',
                                'post_activity_by_action_type_unique', 'post_video_ad_break_ad_impressions',
                                'post_video_ad_break_earnings', 'post_video_ad_break_ad_cpm']
    list_field_ad_set_insights = ['account_currency', 'account_id', 'account_name', 'action_values', 'actions',
                                  'ad_click_actions', 'ad_id', 'ad_impression_actions', 'ad_name', 'adset_id',
                                  'adset_name', 'attribution_setting', 'auction_bid', 'auction_competitiveness',
                                  'auction_max_competitor_bid', 'buying_type', 'campaign_id', 'campaign_name',
                                  'canvas_avg_view_percent', 'canvas_avg_view_time', 'catalog_segment_actions',
                                  'catalog_segment_value', 'catalog_segment_value_mobile_purchase_roas',
                                  'catalog_segment_value_omni_purchase_roas',
                                  'catalog_segment_value_website_purchase_roas', 'clicks', 'conversion_values',
                                  'conversions', 'converted_product_quantity', 'converted_product_value',
                                  'cost_per_15_sec_video_view', 'cost_per_2_sec_continuous_video_view',
                                  'cost_per_action_type', 'cost_per_ad_click', 'cost_per_conversion',
                                  'cost_per_dda_countby_convs', 'cost_per_inline_link_click',
                                  'cost_per_inline_post_engagement', 'cost_per_one_thousand_ad_impression',
                                  'cost_per_outbound_click', 'cost_per_thruplay', 'cost_per_unique_action_type',
                                  'cost_per_unique_click', 'cost_per_unique_conversion',
                                  'cost_per_unique_inline_link_click', 'cost_per_unique_outbound_click', 'cpc', 'cpm',
                                  'cpp', 'created_time', 'ctr', 'date_start', 'date_stop', 'dda_countby_convs',
                                  'dda_results', 'frequency', 'full_view_impressions', 'full_view_reach', 'impressions',
                                  'inline_link_click_ctr', 'inline_link_clicks', 'inline_post_engagement',
                                  'instagram_upcoming_event_reminders_set', 'instant_experience_clicks_to_open',
                                  'instant_experience_clicks_to_start', 'instant_experience_outbound_clicks',
                                  'interactive_component_tap', 'marketing_messages_delivery_rate',
                                  'mobile_app_purchase_roas', 'objective', 'optimization_goal', 'outbound_clicks',
                                  'outbound_clicks_ctr', 'place_page_name', 'purchase_roas',
                                  'qualifying_question_qualify_answer_rate', 'reach', 'social_spend', 'spend',
                                  'updated_time', 'video_30_sec_watched_actions', 'video_avg_time_watched_actions',
                                  'video_continuous_2_sec_watched_actions', 'video_p100_watched_actions',
                                  'video_p25_watched_actions', 'video_p50_watched_actions', 'video_p75_watched_actions',
                                  'video_p95_watched_actions', 'video_play_actions', 'video_play_curve_actions',
                                  'video_play_retention_0_to_15s_actions', 'video_play_retention_20_to_60s_actions',
                                  'video_play_retention_graph_actions', 'video_time_watched_actions', 'website_ctr',
                                  'website_purchase_roas']
    list_field_ad_campaign_insights = ['account_currency', 'account_id', 'account_name', 'action_values', 'actions',
                                       'ad_click_actions', 'ad_id', 'ad_impression_actions', 'ad_name', 'adset_id',
                                       'adset_name', 'attribution_setting', 'auction_bid', 'auction_competitiveness',
                                       'auction_max_competitor_bid', 'buying_type', 'campaign_id', 'campaign_name',
                                       'canvas_avg_view_percent', 'canvas_avg_view_time', 'catalog_segment_actions',
                                       'catalog_segment_value', 'catalog_segment_value_mobile_purchase_roas',
                                       'catalog_segment_value_omni_purchase_roas',
                                       'catalog_segment_value_website_purchase_roas', 'clicks', 'conversion_values',
                                       'conversions', 'converted_product_quantity', 'converted_product_value',
                                       'cost_per_15_sec_video_view', 'cost_per_2_sec_continuous_video_view',
                                       'cost_per_action_type', 'cost_per_ad_click', 'cost_per_conversion',
                                       'cost_per_dda_countby_convs', 'cost_per_inline_link_click',
                                       'cost_per_inline_post_engagement', 'cost_per_one_thousand_ad_impression',
                                       'cost_per_outbound_click', 'cost_per_thruplay', 'cost_per_unique_action_type',
                                       'cost_per_unique_click', 'cost_per_unique_conversion',
                                       'cost_per_unique_inline_link_click', 'cost_per_unique_outbound_click', 'cpc',
                                       'cpm',
                                       'cpp', 'created_time', 'ctr', 'date_start', 'date_stop', 'dda_countby_convs',
                                       'dda_results', 'frequency', 'full_view_impressions', 'full_view_reach',
                                       'impressions',
                                       'inline_link_click_ctr', 'inline_link_clicks', 'inline_post_engagement',
                                       'instagram_upcoming_event_reminders_set', 'instant_experience_clicks_to_open',
                                       'instant_experience_clicks_to_start', 'instant_experience_outbound_clicks',
                                       'interactive_component_tap', 'marketing_messages_delivery_rate',
                                       'mobile_app_purchase_roas', 'objective', 'optimization_goal', 'outbound_clicks',
                                       'outbound_clicks_ctr', 'place_page_name', 'purchase_roas',
                                       'qualifying_question_qualify_answer_rate', 'reach', 'social_spend', 'spend',
                                       'updated_time', 'video_30_sec_watched_actions', 'video_avg_time_watched_actions',
                                       'video_continuous_2_sec_watched_actions', 'video_p100_watched_actions',
                                       'video_p25_watched_actions', 'video_p50_watched_actions',
                                       'video_p75_watched_actions',
                                       'video_p95_watched_actions', 'video_play_actions', 'video_play_curve_actions',
                                       'video_play_retention_0_to_15s_actions',
                                       'video_play_retention_20_to_60s_actions',
                                       'video_play_retention_graph_actions', 'video_time_watched_actions',
                                       'website_ctr',
                                       'website_purchase_roas']
    list_field_instagram_insights = ['impressions', 'reach', 'total_interactions', 'accounts_engaged', 'likes',
                                     'comments', 'saves', 'shares', 'replies', 'follows_and_unfollows',
                                     'profile_links_taps']
    list_field_instagram_demographic_insights = ['engaged_audience_demographics', 'reached_audience_demographics',
                                               'follower_demographics']
    
    def __init__(self, logger: Logger):
        self.logger = logger

        self.session = self.request_session()
    @staticmethod
    def request_session():
        session = requests.Session()
        """
        Create a session with retry handling

        :param session (_type_, optional): user session.
        :returns session: request session
        """
        if session is None:
            session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(pool_connections=config.FACEBOOK_POOL_CONNECTIONS,
                                                    pool_maxsize=config.FACEBOOK_POOL_MAX_SIZE)
            session.mount('http://', adapter)
            session.mount('https://', adapter)

        return session
    
    @staticmethod
    def get_data(path_id, params={}, data_key=None, paging_key=None, has_sleep=False, attach_params=False):
        url = f"{config.FACEBOOK_BASE_URL}/{config.FACEBOOK_API_VERSION}/{path_id}"
        datas = []
        has_more = True
        session = requests.Session()
        page_index = 1
        try:
            while has_more:
                print(f"Get data from endpoint: {path_id} with page index: {page_index}")
                response = session.get(url=url, params=params, timeout=config.FACEBOOK_API_TIMEOUT)
                print(url)

                if response.status_code == 200:
                    response_json = response.json()

                    if data_key:
                        data_json = response_json.get(data_key, {})
                        if attach_params:
                            if isinstance(data_json, list):
                                for item in data_json:
                                    item['params'] = params
                            else:
                                data_json['params'] = params

                        datas.extend(data_json) if isinstance(data_json, list) else datas.append(data_json)
                    else:
                        if attach_params:
                            if isinstance(response_json, list):
                                for item in response_json:
                                    item['params'] = params
                            else:
                                response_json['params'] = params
                        datas.extend(response_json) if isinstance(response_json, list) else datas.append(response_json)

                    if paging_key:
                        response_json = response_json.get(paging_key, {})

                    if response_json.get('paging', {}).get('next', None):
                        url = response_json.get('paging', {}).get('next', None)

                        # Tìm giá trị until để kiểm tra so với mong muốn đầu vào, nếu có.
                        match = re.search(r'until=(\d+)', url)
                        until_value = match.group(1) if match else None
                        if until_value and params.get('until', None) and int(until_value) >= int(
                                params.get('until', None)):
                            has_more = False
                    else:
                        has_more = False
                else:
                    message = f"Error when getting record from {path_id} in Facebook, status_code: {response.status_code}, error: {response.text} "
                    raise HTTPError(message)

                page_index += 1
                if has_sleep and page_index % 5 == 0:
                    time.sleep(5)

        except HTTPError as e:
            raise e
            # self.logger.error(f"HTTPError occurred: {e}")
        except RequestException as e:
            error_msg = f"Request exception when getting record from {path_id} in Facebook, error: {e}"
            raise Exception(error_msg)
            # self.logger.error(error_msg)
        except Exception as e:
            raise e
            # self.logger.error(f"An unexpected error occurred: {e}")

        return datas
    @staticmethod
    def parse_ad_account(ad_account, **kwargs):
        """
        Parse ad account data from Facebook Graph API.
        """
        record = {key: None for key in FacebookHelper.list_fields_ad_account}
        for key, value in ad_account.items():
            record[key] = value
        for key, value in kwargs.items():
            record[key] = value

        return record

    @staticmethod
    def parse_ad_campaign(ad_campaign, **kwargs):
        """
        Parse ad account data from Facebook Graph API.
        """
        record = {key: None for key in FacebookHelper.list_fields_campaigns}
        for key, value in ad_campaign.items():
            record[key] = value
        for key, value in kwargs.items():
            record[key] = value

        return record
# data = get_data('act_123456789/campaigns')
# print(data)

# account = parse_ad_account({'id': '12345', 'name': 'My Ad Account'}, currency='USD')
# print(account)

# campaign = parse_ad_campaign({'id': '98765', 'name': 'My Campaign'}, budget=5000)
# print(campaign)
