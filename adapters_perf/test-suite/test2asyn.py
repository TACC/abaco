import requests
import statistics
import concurrent.futures

def bacoreq(base_url):
    r1 = requests.get(f'{base_url}/app')
    return r1

base_url = "http://localhost:8000"
trials=100
time_data=[]
time_stats={}

with concurrent.futures.ThreadPoolExecutor() as executor:
    f1 = [executor.submit(bacoreq,base_url) for i in range(trials)]
    for f in concurrent.futures.as_completed(f1):
        time_data.append((f1.elapsed.total_seconds()) * 1000)


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