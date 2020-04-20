# Change Log
All notable changes to this project will be documented in this file.

## 1.5.4 - 2020-04-20
### Added
- No change.

### Changed
- Actors are no longer put into ERROR state when OAuth (APIM) client generation fails.
- The AgaveClientsService.create() method now tried to delete a "partially created" client for which credential generation 
failed. 

### Removed
- No change.


## 1.5.3 - 2020-04-08
### Added
- No change.

### Changed
- Compiled with an update to agaveflask core lib which adds support for the portals-api and 3dem tenants. 

### Removed
- No change.


## 1.5.2 - 2020-04-08
### Added
- No change.

### Changed
- Add second check of the globals.keep_running sentinel in the main worker thread (thread 1) to shrink the time 
window between a worker receiving a shutdown signal (in thread 2) and relaying that to thread 1, particularl after a 
new actor message was received (in thread 1). 
The previous, larger time window resulted in a race condition that could cause an actor message to get 
"partially" processed while the worker was being shut down. In particular, refreshing the token in thread 1 could fail if 
thread 2 had already removed the oauth client.
- Add retry logic to oauth client generation for a new worker; try up to 10 times before giving up and putting the actor 
in an error state.

### Removed
- No change.


## 1.5.1 - 2020-04-05
### Added
- No change.

### Changed
- Fixed a bug resulting in an exception and possibly setting an actor to ERROR state when truncating an execution log. 
- Physically delete worker records from workers store in spawner when a previous or concurrent error prevents the spawner from ever creating/starting the worker containers.
- Workers now try to refresh the access token up to 10 times (with a 2 second sleep between each attempt) before giving up and putting the actor into an ERROR state.
- Fixed an exception (which previously was only logged and swallowed) in the metrics_utils module caused by trying to access a variable that had not been defined under a certain code path.
- Added additional logging in spawner and worker modules.  

### Removed
- No change.


## 1.5.0 - 2019-10-29
### Added
- Added an endpoint `PUT /actors/aliases/{alias}` for updating the 
definition of an alias. Requires `UPDATE` permission for the alias as well as for the actor to which the alias should be defined. 

### Changed
- Fixed a bug where nonces defined for aliases would not be honored when using the alias in the URL (they were only honored when using the actor id assigned to the alias).   
- Fixed issue where autoscaler did not properly scale down worker pools for actors with the `sync` hint. They are now scaled down to 1.
- The permission check on all on all `/aliases/{alias}` endpoints has been updated to require UPDATE on the associated `actor_id`. 
- Fixed issue where the actor's token attribute was not being processed correctly causing tokens to be generated even for actors for which the attribute was false.
- Fixed issue where hypyerlinks in response model for executions were not generated correctly, showing the actor's internal database id instead of the human readable id. 
- Fixed error messaging when using a nonce and the API endpoint+HTTP verb combination do not exist.
- The admin role is now recognized when checking access to certain objects in some edge cases, including when a nonce is used. 

### Removed
- It is no longer possible to create an alias nonce for permission levels UPDATE. 


## 1.4.0 - 2019-09-16
### Added
- Added `hints` attribute to the actor data model, a list of strings representing metadata about an actor. "Official"
Abaco hints will be added over time to provide automatic configuration of actors.
- Added support for the `sync` official hint: when an actor is registered with hint "sync", the Abaco autoscaler will
leave at least one worker in the actor's worker pool up to a tenant-specific period of idle time. This idle time is
configured using the `sync_max_idle_time` within the `[workers]` stanza of the `abaco.conf` file.
- Added a "utilization" endpoint, `GET /actors/utilization`, which returns basic utilization data about the
Abaco cluster.

### Changed
- Changed the way Abaco generates OAuth tokens that it injects into actors by prefixing the username associated with
the token by its userstore's id. This change fixes an issue where other Tapis services (such as profiles) would not
work properly when hit with the token because the associated JWT was not generated properly by WSO2. Otherwise,
this change should be transparent to the end user.
- Fixed an issue where the `PUT /actors/{actor_id}` endpoint did not default the actor's `token` attribute to the tenant
default. Now, if the `token` attribute is missing from the `PUT` message body, Abaco will use the default value for the
tenant or instance.
- An actor's executions list is now initialized when the actor is created to prevent a race condition that can occur 
when multiple client threads try to add the very first execution (i.e., send the first message).
- The `DELETE /actors/aliases/{alias}` now returns a 404 not found if the alias `{alias}` does not exist. 
- Fixed an issue with `GET /actors/{actor_id}/nonces` where nonces created before the 1.1.0 release (which introduced nonces
associated with aliases) were not properly serialized in the response, causing random id's to be generated for the nonces
instead of returning their actual id's. 

### Removed
- No change.


## 1.3.0 - 2019-08-6
### Added
- Added a `token` Boolean attribute to the actor data model, indicating whether a token will be generated for an actor.
When this attribute is False, Abaco will not generate an access token for the actor.

### Changed
- Fixed an issue where the results socket was not writeable by non-root accounts.
- The Abaco API proxy (nginx) now returns properly formatted JSON messages for unhandled 400 and 500 level errors
including bad gateway and timeout errors.
- Fixed various issues associated with Abaco resources not being shut down correctly on actor delete. First, actors now
enter a `SHUTTING_DOWN` status immediately upon receiving a delete request, and this status is recognized by the autoscaler
to prevent workers from being started. Second, workers now enter first `SHUTDOWN_REQUESTED` followed by `SHUTTING_DOWN`
when they have been requested (respectively, received the stop request) to shut down. Spawners now check if a worker is
in `REQUESTED` or `SHUTDOWN_REQUESTED` status before proceeding with starting the worker. Finally, the actor DELETE
API now waits up to 20 seconds for all workers to be shut down and if they have not yet, the delete still returns a 200
but the response message indicates that not all resources were shut down.
- Workers now force halt a running execution when an actor has been deleted; this allows resources to be cleaned up more
efficiently.
- Fixed a rare edge case issue where a worker container would not exit cleanly due to the the second worker_ch thread not checking
the global keep_running boolean properly.
- The abaco.conf file now accepts configurations of the form `{tenant}_default_token` and `default_token` within the `[web]` stanza to provide a
default value for the actor token attribute for tenants, respectively, the global Abaco instance. When a tenant and global0
configuration is set, actors in a given tenant will get the tenant's configuration.
- The abaco.conf file now accepts a `{tenant}_generate_clients` configuration within the `[workers]` stanza that dictates
whether client generation is available for a specific tenant.
- Several log messages were cleaned up and improved.

### Removed
- No change.


## 1.2.2 - 2019-07-28
### Added
- No change

### Changed
- Fixed an issue where in a certain edge case, workers were not exiting properly due to a bug trying to clean up a connection to RabbitMQ.

### Removed
- No change.


## 1.2.1 - 2019-07-22
### Added
- No change

### Changed
- Fixed an issue where in a certain edge case, workers were getting shut down by the autoscaler before executions were getting processed.  
- The abaco.conf now expects a `max_cmd_length` config within the `spawner` stanza which should be an integer and controls how many messages the autoscaler will send to the default command channel at a time.   

### Removed
- No change.


## 1.2.0 - 2019-07-15
### Added
- Added actor events subsystem with events agent that reads from the events queue.
- Added support for actor links to send an actor's events to another actor.
- Added support for an actor webhook property for sending an actor's events as an HTTP POST to an endpoint.
- Added timing data to messages POST processing.

### Changed
- Executions now change to status "RUNNING" as soon as a worker starts the corresponing actor container.
- Force halting an execution fails if the status is not RUNNING.
- Reading and managing nonces associated with aliases requires permissions on both the alias and the actor.
- Spawner now sets actor to READY state before setting worker to READY state to prevent autoscaler from stopping worker before actor is update to READY. 
- Updated ActorMsgQueue to use a new, simpler class, TaskQueue, removing dependency on channelpy.

### Removed
- No change.


## 1.1.0 - 2019-06-18
### Added
- Added support for sending synchronous messages to an actor.
- Added support for creating/managing nonces associated with aliases through a new API: `GET, POST /actors/aliases/{alias}/nonces`. 
- Added support for halting a running execution through a new API endpoint: `DELETE /actors/{actor_id}/executions/{execution_id}`.
- Added support for streaming logs back to the logs service during a running execution so that the user does not have to wait for an execution to complete before seeing logs.

### Changed
- The spawer management of workers has been greatly simplified with a significant reduction in messages between the two agents at start up. Worker status was updated to add additional worker states during start up. Worker state transitions are now validated at the model level.   
- The `abacosamples/wc` word count image has been updated to now send a bytes result on the results channel.
- Improved worker and client cleanup code when actor goes into an ERROR state. 
- Updates to health agent to add additional checks/clean up of clients store.
- Consolidated to a single docker-compose.yml file for local development and upgraded it to v3 docker-compose format.

### Removed
- No change.


## 1.0.0 - 2019-03-18
### Added
- Final updates to the Abaco Autoscaler in preparation for its release.
- Added "actor queues" feature to allow actors to be registered into a specific queue so that dedicated computing resources can be provided for specific groups of actors; updates to the controlers, spawner and health agents were made to support this feature.
- Added a "description" field on nonce objects to ease user management of nonces.
- Added a new "image classifier" sample, `abacosamples/binary_message_classifier`, that uses a pre-trained image classifier algorithm based on tensorflow to classify an image sent as a binary message.  

### Changed
- Aliases are now restricted to a whilelist of characters to prevent issues with the use of non-URL safe characters. 
- Several modules were changed to improve handling of errors such as connection issues with RabbitMQ or socket level errors generated by the Docker daemon. 

### Removed
- No change.


## 0.12.0 - 2019-01-21
### Added
- Add support for actor aliases allowing operators to refer to actors and associated endpoints via a self-defined identifier (alias) instead of the actor id.
- Add support for actor resource limits for cpu and memory. These can be globally configured, and, with admin privileges, overriden on a per-actor basis at registration and update (`max_cpus`/`maxCpus` and `mem_limit`/`memLimit`).
- Add support for endpoint `DELETE /actors/{aid}/messages` to purge an actor's mailbox.
- The fields  `actor_name`, `worker_id`, `container_repo` are now available in the context for an actor execution.
- Add support for atomic list mutations on the Redis store class.
- Grafana config added to Promtheus auto-scaler component.
- `len(clients_store)` is now a Prometheus gauge metric.
- Improved logging in spawner, worker, health, clientg and models modules.

### Changed
- By default, actors are now registered as stateless. This means, by default, the state API will not be available but autoscaling will.
- Improve error handling when clientg process receives an error generating an OAuth client or token.
- Fix bug where workers API reported worker create time incorrectly.
- The locust load test suite application was expanded to allow additional types of actors to be registered and executed; addtionally, bugs were corrected and configuration improved.
- The autoscaler now honors a `max_workers` field for each actor; it also only runs scale up method if the command queue is less than a configurable max length.
- Fixed bug in scale-down method of autoscaler preventing scale down when actor had exactly 1 worker.
- Some aspects of the health process were changed to better integrate with the autoscaler.
- Fixed bug preventing health process from restarting crashed spawner correctly.
- Fixed bug in kill_worker causing database integrity issues when pull_image failed with an exception.
- Worker containers are now named by their actor and worker id for ease of identifying them.
- Fixed a bug where a results channel was not always closed properly, causing undue resource usage.

### Removed
- No change


## 0.11.0 - 2018-10-16
### Added
- A new sleep_loop sample was added for replicating actor executions with varying execution lengths.

### Changed
- The channels module was refactored to give clients more control over acking/nacking messages, and whether to pre-fetch messages. This fixes a bug where messages could get lost when a worker crashes in certain ways.
- The core code was upgraded to Python 3.6.6 and the base images were updated to Alpine 3.8.
- The admin API now returns workers as a list, and a few other small bugs were fixed.
- Several updates and fixes were made to the Admin Dashboard application.

### Removed
- No change


## 0.10.0 - 2018-08-01
### Added
- New endpoints in the Admin API, `/actors/v2/admin/workers` and ``/actors/v2/admin/executions`, for retrieving data
about workers and executions, respectively.
- New `abacosamples/agave_submit_jobs` sample image for submitting a job from an actor.

### Changed
- Fix issue where Spawner process would crash when receiving a Timeout error from the Docker daemon when a compute node
was under heavy load.
- Hardening of various worker actions when compute node is under heavy load, including hardening of stats collection,
results socket creation and teardown, and actor container stopping. Adds significant improvements to exception handling
and retry logic in these failure cases, and puts actor in error states when unrecoverable errors are encountered. Among
other things, these improvements should prevent multiple actor containers from running concurrently under the same
worker.
- Numerous improvements to documentation.

### Removed
- No change


## 0.9.0 - 2018-07-06
### Added
- Extended support for a tenant-specific identity configurations; specifically, enabling use/non-use of
TAS integration at the tenant level as well as use of global UID and GID.

### Changed
- Fixed a reliance on the existence of the Internal/everyone role in the JWT; now, if no roles are present in the JWT,
Abaco inserts the "everyone" role enabling basic access and functionality.

### Removed
- No change


## 0.8.0 - 2018-05-10
### Added
- Added support for a tenant-specific global_mounts config.

### Changed
- Changed RabbitMQ connection handling across all channel objects to greatly reduce cpu load on RabbitMQ server as well
as on worker nodes in the cluster.
- Implemented a stop-no-delete command on the command channel to prevent a race condition when updating an actor's image
that could cause the new worker to be killed.
- Fixed an issue where Docker fails to report container execution finish time when the compute server is under heavy
load. In this case, we note return finish_time as computed from the start_time and the run_time (calculated by Abaco).
- Fixed issues with Actor update: 1) owner can no longer change in case a different user from the original owner
updates the actor image, 2) last_update_time is always updated, and 3) ensure updater has permanent permissions for the
actor.

### Removed
- No change

## 0.7.0 - 2018-04-08
### Added
- Added support for setting max_workers_per_host to prevent overloading.
- Added support for retrieving the TAS GID on a per user basis from the extended profile within Metadata.
- Initial implementation of autoscaling via Prometheus added.
- Additional fields for each execution are now returned in the executions summary.

### Changed
- The routines used when executing an actor container have been simplified to provide better performance and to prevent
some issues such as stats collection generating a UnixHTTPConnectionPool Readtime when compute server is under load.
- Added several safety guards to the health checker code to prevent crashes of the health checker when working with
unexpected data (e.g. when a worker's last_execution is not defined)
- Fixed bug due to message formatting issue in message returned from a POST to the /workers endpoint.

### Removed
- The 'ids' collection has been removed from the executions endpoint response in favor of an 'executions' collections
providing additional fields for each execution.


## 0.6.0 - 2018-03-08
### Added
- Add support for binary messages through a FIFO mount to the actor.
- Add support for a "results" endpoint associated with each execution. Results are read from a Unix Domain socket
mounted into the actor container and streamed to a Results queue specific to the execution.
- Read host id from the environment to support dynamic assignment such as when deploying with kubernetes.
- Add create_time attribute to workers and fix issue with health agents shutting down new workers too quickly if the
worker had not processed an execution.
- An actor's state object can now be an arbitrary JSON-serializable object (not just a dictionary).

### Changed
- Messages to add multiple new workers are now sent as multiple messages to the command queue to add 1 worker. This
distributed commands across multiple spawners better.
- Default expiration time for Results channels has been increased from 100s to 20 minutes.
- Fixed a bug in the auth check that caused certain POST requests to fail with "not authorized" errors when the payload
was not a JSON-dictionary.
- Fixed an issue preventing an actor's state object from being updated correctly.

### Removed
- No change.


## 0.5.2 - 2018-01-26
### Added
- Fixed issue where permissions errors were giving a confusing message about "unrecognized exception".
- Fixed bug causing a worker to be added to the workers_store with the wrong worker_id in a narrow case.
- Fixed an issue where the put_sync in the health check was causing messages to be left on the queue when the worker
had already stopped.
- Fixed issue where requests to update an actor (i.e., PUT requests) were ignoring certain fields (e.g.,
default_environment)
- Fixed bug preventing the Agave OAuth client from being properly instantiated within the actor container when the
actor was launched via a nonce.
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

