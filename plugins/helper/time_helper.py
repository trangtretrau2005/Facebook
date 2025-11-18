from datetime import datetime, date, timezone, timedelta

import pytz
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

from plugins.config import config


def get_current_posix_timestamp():
    current_datetime = datetime.now().astimezone(pytz.timezone(config.DWH_TIMEZONE))
    posix_timestamp = int(current_datetime.timestamp() * 1000)

    return posix_timestamp


def posix_timestamp_to_local_datetime_string(timestamp):
    time_object = datetime.fromtimestamp(timestamp)
    dt_asia_7 = time_object.astimezone(pytz.timezone(config.DWH_TIMEZONE))
    formatted_datetime = dt_asia_7.strftime("%Y-%m-%dT%H:%M:%S.%f")

    return formatted_datetime


def get_start_end_current_date():
    now_local = get_now_local_time()

    start_of_date = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_date = start_of_date + timedelta(days=1) - timedelta(microseconds=1)

    return start_of_date, end_of_date


def get_start_end_current_date_hour_epoch():
    # Get current UTC time
    now_utc = datetime.now(timezone.utc)

    # Define timezone for UTC+7
    timezone_offset = timezone(timedelta(hours=7))

    # Get current datetime in UTC+7
    now_local = now_utc.astimezone(timezone_offset)

    # Get start of current date (set hour, minute, second, microsecond to 0)
    start_of_hour = now_local.replace(minute=0, second=0, microsecond=0)

    # Get the end of the current hour (set minutes, seconds, microseconds to 59)
    end_of_hour = start_of_hour + timedelta(hours=1) - timedelta(microseconds=1)

    # Convert start_of_date to POSIX timestamp (seconds since epoch)
    start_timestamp = start_of_hour.timestamp() * 1000
    end_timestamp = end_of_hour.timestamp() * 1000
    return int(start_timestamp), int(end_timestamp)


def get_start_end_current_date_epoch(date: str = "2024-01-01"):
    """
    Converts a date string in format like "2024-06-02" in UTC+7 to start date epoch and end date epoch
    """
    # Get current datetime in UTC+7
    now_local = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone(offset=timedelta(hours=7)))

    # Get start of current date (set hour, minute, second, microsecond to 0)
    start_of_date = now_local.replace(hour=0, minute=0, second=0, microsecond=0)

    # Get end of current date (set hour to 23, minute, second, microsecond to max)
    end_of_date = start_of_date + timedelta(days=1) - timedelta(microseconds=1)

    # Convert start_of_date to POSIX timestamp (seconds since epoch)
    start_timestamp = start_of_date.strftime("%Y-%m-%d %H:%M:%S")
    end_timestamp = end_of_date.strftime("%Y-%m-%d %H:%M:%S")
    return int(start_timestamp), int(end_timestamp)


def get_start_end_current_date_hour_format(format="%Y-%m-%d %H:%M:%S"):
    # Get current UTC time
    now_utc = datetime.now(timezone.utc)

    # Define timezone for UTC+7
    timezone_offset = timezone(timedelta(hours=7))

    # Get current datetime in UTC+7
    now_local = now_utc.astimezone(timezone_offset)

    # Get start of current date (set hour, minute, second, microsecond to 0)
    start_of_hour = now_local.replace(minute=0, second=0, microsecond=0)

    # Get the end of the current hour (set minutes, seconds, microseconds to 59)
    end_of_hour = start_of_hour + timedelta(hours=1) - timedelta(microseconds=1)

    # Convert start_of_date to POSIX timestamp (seconds since epoch)
    start_timestamp = start_of_hour.strftime(format)
    end_timestamp = end_of_hour.strftime(format)
    return start_timestamp, end_timestamp


def get_start_end_current_date_format(date=None, format="%Y-%m-%d %H:%M:%S"):
    if date is None:
        # Get current UTC time
        now_utc = datetime.now(timezone.utc)

        # Define timezone for UTC+7
        timezone_offset = timezone(timedelta(hours=7))

        # Get current datetime in UTC+7
        now_local = now_utc.astimezone(timezone_offset)
    else:
        # Get current datetime in UTC+7
        now_local = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone(offset=timedelta(hours=7)))

    # Get start of current date (set hour, minute, second, microsecond to 0)
    start_of_date = now_local.replace(hour=0, minute=0, second=0, microsecond=0)

    # Get end of current date (set hour to 23, minute, second, microsecond to max)
    end_of_date = start_of_date + timedelta(days=1) - timedelta(microseconds=1)

    # Convert start_of_date to POSIX timestamp (seconds since epoch)
    start_timestamp = start_of_date.strftime(format)
    end_timestamp = end_of_date.strftime(format)
    return start_timestamp, end_timestamp


def get_start_end_previous_date_hour_format():
    # Get current UTC time
    now_utc = datetime.now(timezone.utc)

    # Define timezone for UTC+7
    timezone_offset = timezone(timedelta(hours=7))

    # Get current datetime in UTC+7
    now_local = now_utc.astimezone(timezone_offset)

    # Get start of current date (set hour, minute, second, microsecond to 0)
    start_of_hour = now_local.replace(minute=0, second=0, microsecond=0)

    # Get the end of the current hour (set minutes, seconds, microseconds to 59)
    end_of_hour = start_of_hour + timedelta(hours=1) - timedelta(microseconds=1)

    # Get the start of the current hour previous date (set minutes, seconds, microseconds to 59)
    start_of_hour_previous_date = start_of_hour - timedelta(days=1)

    # Convert start_of_date to POSIX timestamp (seconds since epoch)
    start_timestamp = start_of_hour_previous_date.strftime("%d/%m/%Y %H:%M:%S")
    end_timestamp = end_of_hour.strftime("%d/%m/%Y %H:%M:%S")
    return start_timestamp, end_timestamp


def get_format_date_hour_current():
    # Get current UTC time
    now_utc = datetime.now(timezone.utc)

    # Define timezone for UTC+7
    timezone_offset = timezone(timedelta(hours=7))

    # Get current datetime in UTC+7
    now_local = now_utc.astimezone(timezone_offset)
    formatted_datetime = now_local.strftime("%Y/%m/%d/%H")
    return formatted_datetime


def convert_to_timestamp_ms(date_str):
    """
    Converts a date string in format "yyyy-mm-dd 00:00:00" in UTC+7 to a timestamp in milliseconds.

    Args:
        date_str: The date string to convert.

    Returns:
        The timestamp in milliseconds.
    """
    # Parse the date string with UTC+7 timezone information
    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone(offset=timedelta(hours=7)))

    # Convert to milliseconds since epoch
    timestamp_ms = dt.timestamp() * 1000

    return int(timestamp_ms)


def convert_date_to_timestamp(date_str):
    """
    Converts a date string in format "yyyy-mm-dd" in UTC+7 to a timestamp in milliseconds.

    Args:
        date_str: The date string to convert.

    Returns:
        The timestamp in milliseconds.
    """
    # Parse the date string with UTC+7 timezone information
    dt = datetime.strptime(date_str + " 00:00:00", "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone(offset=timedelta(hours=7)))

    # Convert to milliseconds since epoch
    timestamp_ms = dt.timestamp() * 1000

    return int(timestamp_ms)


def convert_to_local_datetime_string(datetime_str):
    """
    Converts a date string in format like "2024-06-02T10:06:55.179Z" to in UTC+7 like "2024-06-02T17:06:55.179000"
    """
    dt_utc = parse(datetime_str)
    # dt_utc = dt.replace(tzinfo=pytz.utc)
    dt_asia_7 = dt_utc.astimezone(pytz.timezone(config.DWH_TIMEZONE))
    formatted_datetime = dt_asia_7.strftime("%Y-%m-%dT%H:%M:%S.%f")

    return formatted_datetime


def convert_formated_to_local_datetime_string(datetime_str):
    """
    Converts a date string in format like "18/06/2024 18:50:29" in UTC+7 to datetime string like "2024-06-02T17:06:55.179000"
    """
    dt = datetime.strptime(datetime_str, "%d/%m/%Y %H:%M:%S").replace(tzinfo=timezone(offset=timedelta(hours=7)))

    formatted_datetime = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")

    return formatted_datetime


def convert_to_local_datetime(datetime_str):
    """
    Converts a date string in format like "2024-06-02T10:06:55.179Z" to in UTC+7 like "2024-06-02T17:06:55.179000"
    """
    dt_utc = parse(datetime_str)
    # dt_utc = dt.replace(tzinfo=pytz.utc)
    dt_asia_7 = dt_utc.astimezone(pytz.timezone(config.DWH_TIMEZONE))

    return dt_asia_7


def get_now():
    return datetime.now(pytz.utc)


def get_now_str():
    return datetime.now(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_now_local_time():
    return datetime.now().astimezone(pytz.timezone(config.DWH_TIMEZONE))


def get_now_local_date():
    local_time = get_now_local_time()
    formatted_date = local_time.strftime("%Y-%m-%d")
    return formatted_date


def get_today():
    return datetime.today().replace(tzinfo=pytz.utc)


def get_start_date_of_month(date=get_today()):
    return date.replace(day=1)


def get_end_date_of_month(date=date.today()):
    first_date_next_month = date + relativedelta(months=+1)
    # print(date,first_date_next_month)
    return first_date_next_month.replace(day=1) - timedelta(days=1)


def get_interval_time(is_plus=False, days=0, hours=0, minutes=0, months=0, is_utc=True):
    if is_plus:
        t = datetime.now(pytz.utc) + relativedelta(months=+months, days=+days, hours=+hours, minutes=+minutes)
    else:
        t = datetime.now(pytz.utc) - relativedelta(months=+months, days=+days, hours=+hours, minutes=+minutes)

    if is_utc == False:
        # t = (t + relativedelta(hours=+7)).strftime("%Y-%m-%d %H:%M:%S")
        t = t.strftime("%Y-%m-%d %H:%M:%S")

    return t


def get_interval_time_from(time, is_plus=False, days=0, hours=0, minutes=0, months=0):
    if is_plus:
        return time + relativedelta(months=+months, days=+days, hours=+hours, minutes=+minutes)
    return time - relativedelta(months=+months, days=+days, hours=+hours, minutes=+minutes)


def get_interval_day(day, is_plus=False, days=0, months=0):
    if is_plus:
        return day + relativedelta(days=+days, months=+months)
    return day - relativedelta(days=+days, months=+months)


def convert_to_sql_format(input_datetime):
    return input_datetime.strftime("%Y-%m-%d %H:%M:%S")


def format_api_finan(input_datetime):
    try:
        return input_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    except:
        return datetime.now(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def updated_at():
    return get_now().strftime("%Y-%m-%d %H:%M:%S")


def updated_at_local_time():
    return get_now_local_time().strftime("%Y-%m-%d %H:%M:%S")


def time_obj_from_string(string):
    time_object = datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
    return time_object


def timestamp_to_datetime(timestamp):
    time_object = datetime.fromtimestamp(timestamp)
    return time_object.strftime("%Y-%m-%d %H:%M:%S")


def datetime_to_timestamp(_datetime):
    time_object = datetime.timestamp(_datetime)
    return int(time_object)


def shopee_time_from_string(string):
    return datetime_to_timestamp(time_obj_from_string(string))


def convert_utc_to_local(input_datetime, sql_format=True):
    if sql_format:
        return input_datetime.tz_convert(config.DWH_TIMEZONE).strftime(config.DWH_TIME_FORMAT)
    else:
        return input_datetime.tz_convert(config.DWH_TIMEZONE)


def is_out_of_working_support_hour(from_hour=7, to_hour=22):
    tz = pytz.timezone(config.DWH_TIMEZONE)
    if (datetime.now(tz).hour > from_hour) and (datetime.now(tz).hour < 22):
        return False
    else:
        print("off time")
        return True
