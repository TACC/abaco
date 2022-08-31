import requests
import statistics

base_url = "http://localhost:8000"
trials=100
time_data=list(range(trials-1))
time_stats={}
for i in range(trials-1):
    r4 = requests.get(f'{base_url}/app')
    time_data[i] = (r4.elapsed.total_seconds()) * 1000

#number of calls made
time_stats['n']=trials
#median
time_stats['median']=statistics.median(time_data)
#mean
time_stats['mean']=statistics.mean(time_data)
#standard deviation
time_stats['standard deviation']=statistics.pstdev(time_data)
#max
time_stats['max']=max(time_data)
#min
time_stats['min']=min(time_data)
print(time_stats)