import random
import unittest
from parse import parse
from dateutil.relativedelta import relativedelta
import datetime
import set_next_ex_past

class TestCalc(unittest.TestCase):
    
    ##testing hours
    #if the increment is only one hour then the next cron_next_ex should be now as it needs to execute in this hour
    def test_1hour(self):
        cs = "2021-01-31 05 + 1 hour"
        cnext = "2021-05-20 12"
        result = set_next_ex_past.set_next_ex_past(cs,cnext)
        now = datetime.datetime.utcnow()
        expected = f"{now.year}-{now.month}-{now.day} {now.hour}"
        self.assertEqual(result,expected)

    #if the increment is n hours then the next cron_next_ex could be now or some m(where m < n) hour from now depending on the previous cron_next_ex
    #if the difference between now and the previous cron_next_ex is exactly a multiple of n, than the new cron_next_ex should be now
    #otherwise it should be a now plus the difference between n and the n modulus of (now - precious cron_next_ex)
    # for example if n equals 5 and the last ex was 2 hour ago than the next execution would be 3=( 5 - (2 mod 5)) hours from now
    def test_nhours(self):
        #we make the n a random integer from 2 to 1000 to capture a large variety of possibilities everytime we test
        n = random.randint(2,1000)
        cs = f"2021-01-31 05 + {n} hours"
        cnext = "2021-05-20 12"             #anything from the past
        result = set_next_ex_past.set_next_ex_past(cs,cnext)
        #finding the expected
        # Parse the cron_next_ex into another list of the form [year, month, day, hour]
        cron_nextex_parsed = parse("{}-{}-{} {}", cnext)
        # Create a datetime object from the cron_next_ex_parsed list
        cron_datetime = datetime.datetime(int(cron_nextex_parsed[0]), int(cron_nextex_parsed[1]), \
            int(cron_nextex_parsed[2]), int(cron_nextex_parsed[3]))
        # Create a datetime object for current time in cron schedule format
        now = datetime.datetime.utcnow()
        now = datetime.datetime(now.year, now.month, now.day, now.hour)

        time_difference_seconds = (now-cron_datetime).total_seconds()
        time_difference = time_difference_seconds//3600
        modulus = time_difference % n
        if modulus == 0:
            timeleft = 0
        else:
            timeleft = n - modulus
        expected_datetime = now + datetime.timedelta(hours=timeleft)
        expected = f"{expected_datetime.year}-{expected_datetime.month}-{expected_datetime.day} {expected_datetime.hour}"
        self.assertEqual(result,expected)

    ##testing days
    #if the increment is only one day then the next cron_next_ex could be now if the hour it is suppose to execute match up with the previous
    #cron_next_ex otherwise if the hours is a bit in the future than the next cron_next_ex will be in the same day but a bit in the future
    # or if the hour is a bit below that cron__next_ex than the next execution will happen the next day
    def test_1day(self):
        cs = "2021-01-31 05 + 1 day"
        cnext = "2021-05-20 12"     #could be anything in the past
        result = set_next_ex_past.set_next_ex_past(cs,cnext)
        now = datetime.datetime.utcnow()
        #the hour needs to be the same as the previous cnext no matter how the day is incremented
        if now.hour == 12 or now.hour < 12:
            expected = f"{now.year}-{now.month}-{now.day} {12}"
        else:
            now = now + datetime.timedelta(days=1)
            expected = f"{now.year}-{now.month}-{now.day} {12}"
        self.assertEqual(result,expected)
    
    #if the increment is n days then the next cron_next_ex could be now or some m(where m < n) day from now depending on the previous cron_next_ex
    #if now is exactly a multiple of n from the previous cron_next_ex, than the new cron_next_ex should be now
    #otherwise it should be a now plus the difference between n and the n modulus of (now - precious cron_next_ex)
    # for example if n equals 5 and the last ex was 2 day ago than the next execution would be 3( 5 - (2 mod 5)) days from now
    def test_ndays(self):
        #we make the n a random integer from 2 to 1000 to capture a large variety of possibilities everytime we test
        n = random.randint(2,1000)
        cs = f"2021-01-31 05 + {n} days"
        cnext = "2021-05-20 12"
        result = set_next_ex_past.set_next_ex_past(cs,cnext)
        #finding the expected
        # Parse the cron_next_ex into another list of the form [year, month, day, hour]
        cron_nextex_parsed = parse("{}-{}-{} {}", cnext)
        # Create a datetime object from the cron_next_ex_parsed list
        cron_datetime = datetime.datetime(int(cron_nextex_parsed[0]), int(cron_nextex_parsed[1]), \
            int(cron_nextex_parsed[2]), int(cron_nextex_parsed[3]))
        # Create a datetime object for current time in cron schedule format
        now = datetime.datetime.utcnow()
        now = datetime.datetime(now.year, now.month, now.day, now.hour)
        time_difference_seconds = (now-cron_datetime).total_seconds()
        time_difference = time_difference_seconds/3600/24
        modulus = time_difference % n
        if modulus == 0:
            timeleft = 0
        else:
            timeleft = n - modulus
        expected_datetime = now + datetime.timedelta(days=timeleft)
        if timeleft < 1:
            if expected_datetime.hour == 12 or expected_datetime.hour < 12:
                expected = f"{expected_datetime.year}-{expected_datetime.month}-{expected_datetime.day} {12}"
            else:
                expected_datetime = expected_datetime + datetime.timedelta(days=n)
                expected = f"{expected_datetime.year}-{expected_datetime.month}-{expected_datetime.day} {12}"
        else:
            expected = f"{expected_datetime.year}-{expected_datetime.month}-{expected_datetime.day} {12}"
        self.assertEqual(result,expected)
    
    ##testing weeks
    #if the increment is only one week then the next cron_next_ex could just be incremented as ndays function except now n=7 specifically
    #technically you could also just treat the 1days functions as nhours(n=24) function and we could treat the ndays function as an n*hour function
    #with n*=24(n) hours. 
    def test_1week(self):
        n=7
        cs = "2021-01-31 05 + 1 week"
        cnext = "2021-05-20 12"
        result = set_next_ex_past.set_next_ex_past(cs,cnext)
        #finding the expected
        # Parse the cron_next_ex into another list of the form [year, month, day, hour]
        cron_nextex_parsed = parse("{}-{}-{} {}", cnext)
        # Create a datetime object from the cron_next_ex_parsed list
        cron_datetime = datetime.datetime(int(cron_nextex_parsed[0]), int(cron_nextex_parsed[1]), \
            int(cron_nextex_parsed[2]), int(cron_nextex_parsed[3]))
        # Create a datetime object for current time in cron schedule format
        now = datetime.datetime.utcnow()
        now = datetime.datetime(now.year, now.month, now.day, now.hour)
        time_difference_seconds = (now-cron_datetime).total_seconds()
        time_difference = time_difference_seconds/3600/24
        modulus = time_difference % n
        if modulus == 0:
            timeleft = 0
        else:
            timeleft = n - modulus
        expected_datetime = now + datetime.timedelta(days=timeleft)
        #the hour needs to be the same as the previous cnext no matter how the day is incremented
        if timeleft < 1:
            if expected_datetime.hour == 12 or expected_datetime.hour < 12:
                expected = f"{expected_datetime.year}-{expected_datetime.month}-{expected_datetime.day} {12}"
            else:
                expected_datetime = expected_datetime + datetime.timedelta(days=n)
                expected = f"{expected_datetime.year}-{expected_datetime.month}-{expected_datetime.day} {12}"
        else:
            expected = f"{expected_datetime.year}-{expected_datetime.month}-{expected_datetime.day} {12}"
        self.assertEqual(result,expected)
        self.assertEqual(result,expected)
    
    def test_nweek(self):
    #as mentioned earlier this function should just be the n_days function with n=7*n
    #we make the n a random integer from 2 to 1000 to capture a large variety of possibilities everytime we test
        n = random.randint(2,1000)
        n_ = 7*n
        cs = f"2021-01-31 05 + {n} weeks"
        cnext = "2021-05-20 12"
        result = set_next_ex_past.set_next_ex_past(cs,cnext)
        #finding the expected
        # Parse the cron_next_ex into another list of the form [year, month, day, hour]
        cron_nextex_parsed = parse("{}-{}-{} {}", cnext)
        # Create a datetime object from the cron_next_ex_parsed list
        cron_datetime = datetime.datetime(int(cron_nextex_parsed[0]), int(cron_nextex_parsed[1]), \
            int(cron_nextex_parsed[2]), int(cron_nextex_parsed[3]))
        # Create a datetime object for current time in cron schedule format
        now = datetime.datetime.utcnow()
        now = datetime.datetime(now.year, now.month, now.day, now.hour)
        time_difference_seconds = (now-cron_datetime).total_seconds()
        time_difference = time_difference_seconds//3600/24
        modulus = time_difference % n_
        if modulus == 0:
            timeleft = 0
        else:
            timeleft = n_ - modulus
        expected_datetime = now + datetime.timedelta(days=timeleft)
        if timeleft < 1:
            if expected_datetime.hour == 12 or expected_datetime.hour < 12:
                expected = f"{expected_datetime.year}-{expected_datetime.month}-{expected_datetime.day} {12}"
            else:
                expected_datetime = expected_datetime + datetime.timedelta(days=n_)
                expected = f"{expected_datetime.year}-{expected_datetime.month}-{expected_datetime.day} {12}"
        else:
            expected = f"{expected_datetime.year}-{expected_datetime.month}-{expected_datetime.day} {12}"
        self.assertEqual(result,expected)
        self.assertEqual(result,expected)

    ##testing months
    #this is when we can do some real time math
    def test_1month(self):
        cs = "2021-01-31 05 + 1 month"
        cnext = "2021-05-20 12"
        result = set_next_ex_past.set_next_ex_past(cs,cnext)
        now = datetime.datetime.utcnow()
        #the hour and day needs to be the same as the previous cnext unless the day is greater than 28
        #checking days
        if now.day > 20:
            expected = f"{now.year}-{now.month+1}-{20} {12}"
        elif now.day == 20:
            #if the day is the same we have to check the hours
            if now.hour > 12:
                now = now + relativedelta(months=+1)
                expected = f"{now.year}-{now.month}-{20} {12}"
            else:
                expected = f"{now.year}-{now.month}-{20} {12}"
        else:
            expected = f"{now.year}-{now.month}-{20} {12}"
        self.assertEqual(result,expected)

    # this function is to test nmonth when the day is less than or equal to 28
    def test_nmonth(self):
        n = random.randint(2,1000)
        cs = f"2021-01-31 05 + {n} months"
        cnext = "2021-04-20 12"
        result = set_next_ex_past.set_next_ex_past(cs,cnext)
        #finding the expected
        # Parse the cron_next_ex into another list of the form [year, month, day, hour]
        cron_nextex_parsed = parse("{}-{}-{} {}", cnext)
        # Create a datetime object from the cron_next_ex_parsed list
        cron_datetime = datetime.datetime(int(cron_nextex_parsed[0]), int(cron_nextex_parsed[1]), \
            int(cron_nextex_parsed[2]), int(cron_nextex_parsed[3]))
        now = datetime.datetime.utcnow()
        # since the difference in month is only relative we also need to add the difference in years
        yeardiff = now.year-cron_datetime.year
        monthdiff = now.month-cron_datetime.month
        #truedifference in the number of months is the relative difference of the months plus the difference in years
        #we also have to add 1 due to now also counting
        truediff = (yeardiff*12+monthdiff+1)
        modulus = truediff % n
        if modulus == 0:
            timeleft = 0
        else:
            timeleft = n-modulus
        now = now + relativedelta(months=+timeleft)
        #the hour and day needs to be the same as the previous cnext unless the day is greater than 28
        #checking days
        if now.day > 20:
            now = now + relativedelta(months=+1)
            expected = f"{now.year}-{now.month}-{20} {12}"
        elif now.day == 20:
            #if the day is the same we have to check the hours
            if now.hour > 12:
                now = now + relativedelta(months=+1)
                expected = f"{now.year}-{now.month}-{20} {12}"
            else:
                expected = f"{now.year}-{now.month}-{20} {12}"
        else:
            expected = f"{now.year}-{now.month}-{20} {12}"
        self.assertEqual(result,expected)

    #day is greater than 28; Depending on the n, if the day is 31 than it might decrease to 30 and if it crosses to february
    #the day will decrease to 29 or 28 depending on if its a leap year or not.
    #eventually it might decrease to 28 and will remain at 28
    #we know the relative delta fuction does just that when incrementing by months as that is the function used in
    #the original set_next_ex. We will try out different values of n to confirm that the fucntion set_next_ex_past
    #returns what we would expect it to return. 
    def test_day28plusmonth(self):
        n = 5
        p = 28
        cs = f"2021-01-31 05 + {n} months"
        cnext = f"2012-03-{p} 12"
        result = set_next_ex_past.set_next_ex_past(cs,cnext)
        #finding the expected
        # Parse the cron_next_ex into another list of the form [year, month, day, hour]
        cron_nextex_parsed = parse("{}-{}-{} {}", cnext)
        # Create a datetime object from the cron_next_ex_parsed list
        cron_datetime = datetime.datetime(int(cron_nextex_parsed[0]), int(cron_nextex_parsed[1]), \
            int(cron_nextex_parsed[2]), int(cron_nextex_parsed[3]))
        now = datetime.datetime.utcnow()
        # since the difference in month is only relative we also need to add the difference in years
        yeardiff = now.year-cron_datetime.year
        monthdiff = now.month-cron_datetime.month
        #truedifference in the number of months is the relative difference of the months plus the difference in years
        truediff = (yeardiff*12+monthdiff+1)
        modulus = truediff % n
        if modulus == 0:
            timeleft = 0
        else:
            timeleft = n - modulus
        now = now + relativedelta(months=+timeleft)
        #the hour and day needs to be the same as the previous cnext unless the day is greater than 28
        #checking days
        if now.day > 28:
            now = now + relativedelta(months=+1)
            expected = f"{now.year}-{now.month}-{28} {12}"
        elif now.day == 28:
            #if the day is the same we have to check the hours
            if now.hour > 12:
                now = now + relativedelta(months=+1)
                expected = f"{now.year}-{now.month}-{28} {12}"
            else:
                expected = f"{now.year}-{now.month}-{28} {12}"
        else:
            expected = f"{now.year}-{now.month}-{p} {12}"
        self.assertEqual(result,expected)
    
    def test_day28plusmonth2(self):
        n = 2
        p = 31
        cs = f"2021-01-31 05 + {n} months"
        cnext = f"2012-03-{p} 12"
        result = set_next_ex_past.set_next_ex_past(cs,cnext)
        #finding the expected
        # Parse the cron_next_ex into another list of the form [year, month, day, hour]
        cron_nextex_parsed = parse("{}-{}-{} {}", cnext)
        # Create a datetime object from the cron_next_ex_parsed list
        cron_datetime = datetime.datetime(int(cron_nextex_parsed[0]), int(cron_nextex_parsed[1]), \
            int(cron_nextex_parsed[2]), int(cron_nextex_parsed[3]))
        now = datetime.datetime.utcnow()
        # since the difference in month is only relative we also need to add the difference in years
        yeardiff = now.year-cron_datetime.year
        monthdiff = now.month-cron_datetime.month
        #truedifference in the number of months is the relative difference of the months plus the difference in years
        truediff = (yeardiff*12+monthdiff)
        modulus = truediff % n
        if modulus == 0:
            timeleft = 0
        else:
            timeleft = n - modulus
        now = now + relativedelta(months=+timeleft)
        #the hour and day needs to be the same as the previous cnext unless the day is greater than 28
        #checking days
        if now.day > 30:
            now = now + relativedelta(months=+1)
            expected = f"{now.year}-{now.month}-{30} {12}"
        elif now.day == 30:
            #if the day is the same we have to check the hours
            if now.hour > 12:
                now = now + relativedelta(months=+1)
                expected = f"{now.year}-{now.month}-{30} {12}"
            else:
                expected = f"{now.year}-{now.month}-{30} {12}"
        else:
            expected = f"{now.year}-{now.month}-{30} {12}"
        self.assertEqual(result,expected)
    
    def test_day28plusmonth3(self):
        n = 4
        p = 31
        cs = f"2021-01-31 05 + {n} months"
        cnext = f"2020-03-{p} 12"
        result = set_next_ex_past.set_next_ex_past(cs,cnext)
        #finding the expected
        # Parse the cron_next_ex into another list of the form [year, month, day, hour]
        cron_nextex_parsed = parse("{}-{}-{} {}", cnext)
        # Create a datetime object from the cron_next_ex_parsed list
        cron_datetime = datetime.datetime(int(cron_nextex_parsed[0]), int(cron_nextex_parsed[1]), \
            int(cron_nextex_parsed[2]), int(cron_nextex_parsed[3]))
        now = datetime.datetime.utcnow()
        # since the difference in month is only relative we also need to add the difference in years
        yeardiff = now.year-cron_datetime.year
        monthdiff = now.month-cron_datetime.month
        #truedifference in the number of months is the relative difference of the months plus the difference in years
        truediff = (yeardiff*12+monthdiff)
        modulus = truediff % n
        if modulus == 0:
            timeleft = 0
        else:
            timeleft = n - modulus
        now = now + relativedelta(months=+timeleft)
        #the hour and day needs to be the same as the previous cnext unless the day is greater than 28
        #checking days
        if now.day > 30:
            now = now + relativedelta(months=+1)
            expected = f"{now.year}-{now.month}-{30} {12}"
        elif now.day == 30:
            #if the day is the same we have to check the hours
            if now.hour > 12:
                now = now + relativedelta(months=+1)
                expected = f"{now.year}-{now.month}-{30} {12}"
            else:
                expected = f"{now.year}-{now.month}-{30} {12}"
        else:
            expected = f"{now.year}-{now.month}-{30} {12}"
        self.assertEqual(result,expected)

    
if __name__ == "__main__":
    unittest.main()