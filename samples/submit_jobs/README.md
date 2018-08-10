## Submit Job Actor Sample ##
# Image: abacosamples/agave_submit_job

This sample demonstrates how to use the agave client to submit a job on to an execution system. This actor
expects a JSON message containing a description of the Agave job to submit, as expected by the Jobs service.

# Example Usage #

```bash
$ curl -H "Content-type: application/json" -H "Authorization: Bearer $tok" -d '{"name": "Abaco-submitted-job", "appId": "wc-0.0.1", "inputs": ["agave://Agave-diagnostics-storage-ds/test.txt"], "archive": false}' $base/actors/v2/$aid/messages
```


