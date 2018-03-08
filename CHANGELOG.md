# Change Log
All notable changes to this project will be documented in this file.

## 0.6.0 - 2018-03-08
### Added
- Add support for binary messages through a FIFO mount to the actor.
- Add support for a "results" endpoint associated with each execution. Results are read from a Unix Domain socket mounted into the actor container and streamed to a Results queue specific to the execution.
- Read host id from the environment to support dynamic assignment such as when deploying with kubernetes.
- Add create_time attribute to workers and fix issue with health agents shutting down new workers too quickly if the worker had not processed an execution.
- An actor's state object can now be an arbitrary JSON-serializable object (not just a dictionary).

### Changed
- Messages to add multiple new workers are now sent as multiple messages to the command queue to add 1 worker. This distributed commands across multiple spawners better.
- Default expiration time for Results channels has been increased from 100s to 20 minutes.
- Fixed a bug in the auth check that caused certain POST requests to fail with "not authorized" errors when the payload was not a JSON-dictionary.
- Fixed an issue preventing an actor's state object from being updated correctly.

### Removed
- No change.


## 0.5.2 - 2018-01-26
### Added
- Fixed issue where permissions errors were giving a confusing message about "unrecognized exception".
- Fixed bug causing a worker to be added to the workers_store with the wrong worker_id in a narrow case.
- Fixed an issue where the put_sync in the health check was causing messages to be left on the queue when the worker had already stopped.
- Fixed issue where requests to update an actor (i.e., PUT requests) were ignoring certain fields (e.g., default_environment)
- Fixed bug preventing the Agave OAuth client from being properly instantiated within the actor container when the actor was launched via a nonce.
- Add shutdown_all_workers convenience utility.
- Several tests added, specifically to validate behavior when invalid inputs were provided.


### Changed
- No change.

### Removed
- No change.


## 0.5.1 - 2018-01-18
### Added
- Fixed issue (#24) where updating an actor caused mounts to disappear.
- Fixed issue (#25) where an actor's status message was not reset when it left an error state.
- Made the user role for "basic" level access configurable (#26).

### Changed
- Turned off "check_workers_store" checks in health module until an optimal approach to data cleanup can be found.

### Removed
- No change.


## 0.5.0 - 2018-01-08
### Added
- Initial external release.

### Changed
- No change.

### Removed
- No change.

