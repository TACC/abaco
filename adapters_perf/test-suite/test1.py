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
JWT = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJqdGkiOiIzMWU1ZTM1Ny02YTRkLTQ5N2MtYWNmMC01MzA4MDcxODdiZTkiLCJpc3MiOiJodHRwczovL2Rldi5kZXZlbG9wLnRhcGlzLmlvL3YzL3Rva2VucyIsInN1YiI6InRlc3R1c2VyMkBkZXYiLCJ0YXBpcy90ZW5hbnRfaWQiOiJkZXYiLCJ0YXBpcy90b2tlbl90eXBlIjoiYWNjZXNzIiwidGFwaXMvZGVsZWdhdGlvbiI6ZmFsc2UsInRhcGlzL2RlbGVnYXRpb25fc3ViIjpudWxsLCJ0YXBpcy91c2VybmFtZSI6InRlc3R1c2VyMiIsInRhcGlzL2FjY291bnRfdHlwZSI6InVzZXIiLCJleHAiOjE2NjEyMjEzOTcsInRhcGlzL2NsaWVudF9pZCI6bnVsbCwidGFwaXMvZ3JhbnRfdHlwZSI6InBhc3N3b3JkIn0.KDvl8KNXyb25iblAf4lHWPdNaaLo-hkiWAIkF9VgRROFglHx5z-2MZyM7h-5s4z0k9PI18CxNfe89Z5B1mxpLLj-0QY62e7Py2PkHQzw7My578KNLGCxUWPxUsMbBjRKyYnHxiKPTbpyDauu2eiJzlyXTkdGHuiDoOH5lcg2sb3opVaV3rUqujopp2l6H3Pp0fbggfpPv-DuG27FDF2D5vyx1qG91z0i0mCa8vzhnHve30kkAicSwQCylLOrckDWkunkg7PI7ilJPU0RIsJTCM8zBF6UlKEe1B1K3sI8LxnRp3jC1ZXtAxgpcIE-vh8gQELMLa6ujD8sSpZA8C1DyQ"

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