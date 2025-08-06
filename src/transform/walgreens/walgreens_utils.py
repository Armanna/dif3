import pytz
from datetime import timedelta, datetime

def calculate_time_diff_utc_cst6cdt(period_start, period_end):
    """
    this function calculates time diff in hours between utc and cst6cdt time
    need to caluclate both for period start and period end since time diff can by different
    at the begining and at the end of the period because of Central Standard Time/Central Daylight Time switch
    """
    cst6cdt = pytz.timezone('US/Central')
    diff_dict = {}
    for name, period in {'start': period_start, 'end': period_end}.items():
        utc_date = period.replace(tzinfo=pytz.utc)
        # Convert the UTC time to a timezone-aware datetime object in CST6CDT
        cst6cdt_date = period.replace(tzinfo=pytz.utc).astimezone(cst6cdt)
        # Format the datetime objects as strings
        utc_date_str = utc_date.strftime('%Y-%m-%d %H:%M:%S')
        cst6cdt_date_str = cst6cdt_date.strftime('%Y-%m-%d %H:%M:%S')
        # Convert the formatted strings back to datetime objects
        utc_date = datetime.strptime(utc_date_str, '%Y-%m-%d %H:%M:%S')
        cst6cdt_date = datetime.strptime(cst6cdt_date_str, '%Y-%m-%d %H:%M:%S')
        # Calculate the time difference in seconds
        time_difference = (cst6cdt_date - utc_date).total_seconds()
        # Round and get the absolute value of the time difference in hours
        rounded_time_difference_hours = round(abs(time_difference) / 3600)
        diff_dict.update({name: rounded_time_difference_hours})
    return diff_dict

def convert_to_utc_plus_6(period_start, period_end):
    """
    adjust period_start and period_end for walgreens in accordance with cst6cdt time
    """
    time_diff_hours = calculate_time_diff_utc_cst6cdt(period_start, period_end)
    period_start_diff = period_start + timedelta(hours=time_diff_hours['start'])
    period_end_diff = (period_end + timedelta(days=1)) + timedelta(hours=time_diff_hours['end']) - timedelta(seconds=1)
    return period_start_diff, period_end_diff
