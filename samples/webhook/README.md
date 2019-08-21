# Webhook Sample
### Image: abacosamples/webhook
A webhook image that allows for users to link to this and hook into a specified url.  
This image provides additional features that the generic webhook
feature does not. This image allows for retry logic, timeout logic, headers for
authentication, and a backoff factor.
 
This image utilizes urllib3 to enable the retries feature. 
 
This image utilizes environment variables to set variables. URL is a required
variable, while the others are optional and include: HEADERS, RETRIES_VAR,
TIMEOUT_VAR, and BACKOFF_FAC. Environment variables can be set for an actor
by providing a 'default_environment' dictionary at actor initialization as shown below.
 
## Example Usage
##### Creating webhook actor
```python
import json
import requests as r
webhook = r.post("http://localhost:8000/actors", 
                 headers={"x-jwt-assertion-DEV-DEVELOP": exampleJWT,
                 		  "Content-Type": "application/json"},
                 data=json.dumps({"image": "abacosamples/webhooks",
                                  "default_environment":{"URL": exampleURL,
                                                         "HEADERS":{"Authentication": "Bearer exampleOAUTH",
                                                                    "Content-Type": exampleTYPE}}}))
webhook_id = webhook.json()['result']['id']
```
 
##### Creating actor linked to webhook
```python
actor = r.post("http://localhost:8000/actors", 
               headers={"x-jwt-assertion-DEV-DEVELOP": exampleJWT,
                 		"Content-Type": "application/json"},
               data={"image": exampleIMAGE,
                     "link": webhook_id}))
```
Now whenever this linked actor is created or is ran, a message will be sent to the specified URL with information regarding actor events.

If you're looking to this out this feature, [webhook.site](https://webhook.site) allows users to see all information regarding HTTP requests to a unique URL.
