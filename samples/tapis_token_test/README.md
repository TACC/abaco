## List Files Actor Sample ##
# Image: abacosamples/tapis_token_test

This sample demonstrates how to use the Tapis access token injected into the actor container to call an API.

# Example Usage #

1) Register the actor.
```bash
curl -H "Authorization: Bearer $tok" $base/actors/v2 -d "image=abacosamples/tapis_token_test&token=True" |jq
```

2) Send the actor a message

```bash
$ curl -H "Content-type: application/json" -H "Authorization: Bearer $tok" -d '{"runs": 3}' $base/actors/v2/$aid/messages
```


