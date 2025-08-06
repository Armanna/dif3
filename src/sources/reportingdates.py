from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def last_day_of_previous_month():
    last_day_of_previous_month = datetime.utcnow().replace(day=1) - \
        timedelta(days=1)

    return last_day_of_previous_month


def first_day_of_previous_month():
    last_day_of_previous_month = datetime.utcnow().replace(day=1) - \
        timedelta(days=1)
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)

    return first_day_of_previous_month


def last_day_of_previous_quarter():
    # assumes we run this in the first month of the quarter
    last_day_of_previous_quarter = datetime.utcnow().replace(day=1) - \
        timedelta(days=1)

    return last_day_of_previous_quarter


def first_day_of_previous_quarter():
    first_day_of_previous_quarter = datetime.utcnow().replace(day=1) - \
        relativedelta(months=3)

    return first_day_of_previous_quarter


def prior_quarter():
    first_day_of_previous_quarter = datetime.utcnow().replace(day=1) - \
        relativedelta(months=3)

    quarter_number = (first_day_of_previous_quarter.month - 1) // 3

    return "Q" + str(quarter_number) + first_day_of_previous_quarter.strftime("%y")


def last_day_of_previous_year():
    last_day_of_previous_year = datetime.utcnow().replace(month=1, day=1) - \
        timedelta(days=1)

    return last_day_of_previous_year


def first_day_of_previous_year():
    first_day_of_previous_year = datetime.utcnow().replace(month=1, day=1) - \
        relativedelta(years=1)

    return first_day_of_previous_year
