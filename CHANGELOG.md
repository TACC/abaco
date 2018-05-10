# Change Log
All notable changes to this project will be documented in this file.


## 0.8.0 - 2018-05-10
### Added
- Added support for a tenant-specific global_mounts config.

### Changed
- Changed RabbitMQ connection handling across all channel objects to greatly reduce cpu load on RabbitMQ server as well as on worker nodes in the cluster.
- Implemented a stop-no-delete command on the command channel to prevent a race condition when updating an actor's image that could cause the new worker to be killed.
- Fixed an issue where Docker fails to report container execution finish time when the compute server is under heavy load. In this case, we note return finish_time as computed from the start_time and the run_time (calculated by Abaco).
- Fixed issues with Actor update: 1) owner can no longer change in case a different user from the original owner updates the actor image, 2) last_update_time is always updated, and 3) ensure updater has permanent permissions for the actor.

### Removed
- No change

## 0.7.0 - 2018-04-08
### Added
- Added support for setting max_workers_per_host to prevent overloading.
- Added support for retrieving the TAS GID on a per user basis from the extended profile within Metadata.
- Initial implementation of autoscaling via Prometheus added.
- Additional fields for each execution are now returned in the executions summary.

### Changed
- The routines used when executing an actor container have been simplified to provide better performance and to prevent some issues such as stats collection generating a UnixHTTPConnectionPool Readtime when compute server is under load.
- Added several safety guards to the health checker code to prevent crashes of the health checker when working with unexpected data (e.g. when a worker's last_execution is not defined)
- Fixed bug due to message formatting issue in message returned from a POST to the /workers endpoint.

### Removed
- The 'ids' collection has been removed from the executions endpoint response in favor of an 'executions' collections providing additional fields for each execution.


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

