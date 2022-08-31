import requests
import os
import time
import sys
import json
import statistics


def basic_response_checks(rsp):
    data = json.loads(rsp.content.decode('utf-8'))
    result = data['result']
    return result

sys.path.append(os.path.split(os.getcwd())[0])
sys.path.append('/actors')
JWT = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJqdGkiOiIwYmQ4MTZhNC1kNWU0LTQ0MWUtOTUwNi0zODQ1NzQwYzllOGQiLCJpc3MiOiJodHRwczovL2Rldi5kZXZlbG9wLnRhcGlzLmlvL3YzL3Rva2VucyIsInN1YiI6InRlc3R1c2VyMkBkZXYiLCJ0YXBpcy90ZW5hbnRfaWQiOiJkZXYiLCJ0YXBpcy90b2tlbl90eXBlIjoiYWNjZXNzIiwidGFwaXMvZGVsZWdhdGlvbiI6ZmFsc2UsInRhcGlzL2RlbGVnYXRpb25fc3ViIjpudWxsLCJ0YXBpcy91c2VybmFtZSI6InRlc3R1c2VyMiIsInRhcGlzL2FjY291bnRfdHlwZSI6InVzZXIiLCJleHAiOjE2NjEyMTY5NjYsInRhcGlzL2NsaWVudF9pZCI6bnVsbCwidGFwaXMvZ3JhbnRfdHlwZSI6InBhc3N3b3JkIn0.eDW3-jmxRHVWCZGsvfj6BZgCwTUyCQytDK6ekkghgOeof114vBRf-SznuELNX4DAEiP-LANqBqoaC3lpAUw2o3WZfj_mH0bBw9zpN_JxJHlTWAT6npjLpbXWgCAhtF7GhamwUg4ZMWbvzmSYP9QsqOLYaV1cKthyv5ueQ7gyERei_IVTVbcy8bjfo2YldkZ8CMPt1X6-VRQ6AkjKcM8HwuEqEJL91qksUjxd_avHO8Fy6nevXW-Hsb9ErEz3BvXyMxjLoJ22FMUyhANVlSPgYxIrkakmTHP1VL9YAfEqK6WmOicWx1CeSLepf2RlplWO0Z_vB0FUE1Xo2f22tof47A"
base_url = 'http://localhost:5000'
headers={'X-Tapis-Token':JWT}


data = {'image':'nshresth/flask-helloworld'}
r2 = requests.post(f'{base_url}/adapters', headers=headers, data=data)
response=basic_response_checks(r2)
reeeee=response['id']
idx=0
success=False
while idx<20 and not success:
        try:
            rsp = requests.get(f'{base_url}/data', headers=headers)
            result = basic_response_checks(rsp)
            success=True
        except:
            time.sleep(1)
            idx = idx + 1

trials=3
time_data=list(range(trials-1))
time_stats={}
for i in range(trials-1):
    r4 = requests.get(f'{base_url}/adapters/{reeeee}/data', headers=headers)

r5= requests.get(f'http://localhost:5000/adapters/{reeeee}/logs', headers={'X-Tapis-Token':JWT})
Time_breakdowns=basic_response_checks(r5)
listy={'got_adapter':[],'got_server':[],'got_address':[],'got_decoded':[],'got_headers':[],'got_response':[],'total':[]}
for i in Time_breakdowns:
    listy['got_adapter'].append(i['get_adapter'])
    listy['got_address'].append(i['got_address'])
    listy['got_decoded'].append(i['got_decoded'])
    listy['got_headers'].append(i['got_headers'])
    listy['got_response'].append(i['got_response'])
    listy['got_server'].append(i['got_server'])
    listy['total'].append(i['total'])
for j in listy:
    #number of calls made
    time_stats['n']=trials
    #median
    time_stats['median']=statistics.median(listy[j])
    #mean
    time_stats['mean']=statistics.mean(listy[j])
    #standard deviation
    time_stats['standard deviation']=statistics.pstdev(listy[j])
    #max
    time_stats['max']=max(listy[j])
    #min
    time_stats['min']=min(listy[j])
    print(j)
    print(time_stats)
    print('\n')

r5 = requests.delete(f'{base_url}/{reeeee}', headers=headers)
