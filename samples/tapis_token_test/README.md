## List Files Actor Sample ##
# Image: abacosamples/agave_files_list

This sample demonstrates how to use the agave client to list files on a storage system.

# Example Usage #

```bash
$ curl -H "Content-type: application/json" -H "Authorization: Bearer $tok" -d '{"path": "/home/testuser2/testuser2", "system_id": "Agave-diagnostics-storage-ds"}' $base/actors/v2/$aid/messages
```


