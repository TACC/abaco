import datetime
from dateutil.relativedelta import relativedelta
from parse import parse
import timeit

#this function assumes that cron_next_ex is in the past
def set_next_ex_past(cron_schedule, cron_next_ex):
        # Parse cron into [datetime, increment, unit of time]
        cron = cron_schedule
        cron_parsed = parse("{} + {} {}", cron)
        time_increment = int(cron_parsed.fixed[1])
        unit_time = cron_parsed.fixed[2]
        # Parse the cron_next_ex into another list of the form [year, month, day, hour]
        cron_nextex_parsed = parse("{}-{}-{} {}", cron_next_ex)
        # Create a datetime object from the cron_next_ex_parsed list
        cron_datetime = datetime.datetime(int(cron_nextex_parsed[0]), int(cron_nextex_parsed[1]), \
            int(cron_nextex_parsed[2]), int(cron_nextex_parsed[3]))
        # Create a datetime object for current time in cron schedule format
        now = datetime.datetime.utcnow()
        now = datetime.datetime(now.year, now.month, now.day, now.hour)
        #initialize certain variables to report when Cron failed
        timeswherecronfailed=[]
        # Logic for incrementing the next execution, whether unit of time is months, weeks, days, or hours
        # we use a while loop
        if unit_time == "month" or unit_time == "months":
            while cron_datetime<now:                                                        #while cron_next_ex is less than now
                new_cron = f"{cron_datetime.year}-{cron_datetime.month}-{cron_datetime.day} {cron_datetime.hour}"
                timeswherecronfailed.append(new_cron)                                #recording when cron failed
                cron_datetime = cron_datetime + relativedelta(months=+time_increment)       #increment the cron_next_ex value by time increment
        elif unit_time == "week" or unit_time == "weeks":
            while cron_datetime<now:
                new_cron = f"{cron_datetime.year}-{cron_datetime.month}-{cron_datetime.day} {cron_datetime.hour}"
                timeswherecronfailed.append(new_cron)
                cron_datetime = cron_datetime + datetime.timedelta(weeks=time_increment)
        elif unit_time == "day" or unit_time == "days":
            while cron_datetime<now:
                new_cron = f"{cron_datetime.year}-{cron_datetime.month}-{cron_datetime.day} {cron_datetime.hour}"
                timeswherecronfailed.append(new_cron)
                cron_datetime = cron_datetime + datetime.timedelta(days=time_increment)
        elif unit_time == "hour" or unit_time == "hours":
            while cron_datetime<now:
                new_cron = f"{cron_datetime.year}-{cron_datetime.month}-{cron_datetime.day} {cron_datetime.hour}"
                timeswherecronfailed.append(new_cron)
                cron_datetime = cron_datetime + datetime.timedelta(hours=time_increment)
        new_cron = f"{cron_datetime.year}-{cron_datetime.month}-{cron_datetime.day} {cron_datetime.hour}"
        return new_cron


cs = "2021-01-31 05 + 2 month"
cnext = "2020-03-31 12"
start_timer = timeit.default_timer()
k = set_next_ex_past(cs,cnext)
end_timer = timeit.default_timer()

timedata = {'total': (end_timer - start_timer) * 1000}

print(k)