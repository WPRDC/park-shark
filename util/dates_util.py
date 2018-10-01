# This is just a holding area for high-maintenance, possibly unneeded
# functions that would require dateutil to be
# imported into the ETL virtual environment.

from dateutil.easter import * # pip install python-dateutil
from datetime import date, timedelta
#from dateutil.relativedelta import relativedelta

from calendar import monthrange

## A bunch of date calculation functions (not currently in use in the code) ##
def nth_m_day(year,month,n,m):
    # m is the day of the week (where 0 is Monday and 6 is Sunday)
    # This function calculates the date for the nth m-day of a
    # given month/year.
    first = date(year,month,1)
    day_of_the_week = first.weekday()
    delta = (m - day_of_the_week) % 7
    return date(year, month, 1 + (n-1)*7 + delta)

def last_m_day(year,month,m):
    last = date(year,month,monthrange(year,month)[1])
    while last.weekday() != m:
        last -= timedelta(days = 1)
    return last

def is_holiday(date_i):
    year = date_i.year
    holidays = [date(year,1,1), #NEW YEAR'S DAY
    ### HOWEVER, sometimes New Year's Day falls on a weekend and is then observed on Monday. If it falls on a Saturday (a normal non-free parking day), what happens?


    # Really an observed_on() function is needed to offset
    # some holidays correctly.

        nth_m_day(year,1,3,0),#MARTIN LUTHER KING JR'S BIRTHDAY (third Monday of January)
        easter(year)-timedelta(days=2),#GOOD FRIDAY
        last_m_day(year,5,0),#MEMORIAL DAY (last Monday in May)
        date(year,7,4),#INDEPENDENCE DAY (4TH OF JULY)
        # [ ] This could be observed on a different day.
        nth_m_day(year,9,1,0),#LABOR DAY
        # [ ] This could be observed on a different day.
        date(year,11,11),#VETERANS' DAY
        # [ ] This could be observed on a different day.

        nth_m_day(year,11,4,3),#THANKSGIVING DAY
        nth_m_day(year,11,4,4),#DAY AFTER THANKSGIVING
        date(year,12,25),#CHRISTMAS DAY
        # [ ] This could be observed on a different day.
        date(year,12,26)]#DAY AFTER CHRISTMAS
        # [ ] This could be observed on a different day.
    return date_i in holidays

def parking_days_in_month(year,month):
    count = 0
    month_length = monthrange(year,month)[1]
    for day in range(1,month_length+1):
        date_i = date(year,month,day)
        if date_i.weekday() < 6 and not is_holiday(date_i):
            count += 1
    return count

## End of date-calculation functions ##
