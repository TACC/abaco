# RabbitMQ in Abaco

How Abaco uses its broker beyond the defaults: switching the must-not-lose queues to quorum.

## Switching queues to quorum (`rabbit_queue_type: quorum`)

`rabbit_queue_type` (default `classic`) decides what type of queue Abaco declares for
the queues that must not lose a message:

- `actor_msg_<dbid>`: actor inboxes
- `command_channel_<name>`: spawner commands
- `events_channel_<name>`: the events agent

Short-lived per-worker queues (`worker_*`, `spawner_worker_*`, `results_*`) stay classic.

A queue's type is fixed when the queue is first declared. If you flip the flag on a
broker that already has these queues, every redeclare fails with
`406 PRECONDITION_FAILED - inequivalent arg 'x-queue-type'`. In practice that means:

- every `POST /messages` to an existing actor returns 503, and its execution is marked
  ERROR;
- the spawner and events agent loop forever on reconnect;
- workers cannot subscribe.

The failing component logs a CRITICAL line that starts with `queue type mismatch:` and
points to this document. Abaco never deletes queues itself, so you have to run the
migration below.

### Prerequisites

- RabbitMQ 3.8 or newer (this repo pins 3.12.14), with the `quorum_queue` feature flag
  enabled: `rabbitmqctl list_feature_flags name state`.
- The `consumer_timeout` override from `rabbitmq-abaco.conf` must be applied to the
  broker. It is independent of queue type, but don't migrate without it.

### Migration

Replace `<vhost>` with each site's vhost (`abaco_<site_id>`, e.g. `abaco_tacc`).

1. **Stop new work coming in.** Stop or block the `mes` (messages) API, the autoscaler
   and anything else that publishes, so the inboxes stop growing.
2. **Drain.** Let the running workers finish the queued messages. Then wait until every
   affected queue is empty:

   ```
   rabbitmqctl list_queues -p <vhost> name type messages \
     | awk '$1 ~ /^(actor_msg_|command_channel_|events_channel_)/'
   ```
3. **Stop every Abaco component**: the API containers, spawner(s), events, health and the
   workers. Every one of them redeclares these queues on connect.
4. **Delete the classic queues.** They have no `x-expires`, so they never expire on
   their own:

   ```
   rabbitmqctl list_queues -p <vhost> name type \
     | awk '$2 == "classic" && $1 ~ /^(actor_msg_|command_channel_|events_channel_)/ {print $1}' \
     | while read q; do rabbitmqctl delete_queue -p <vhost> "$q"; done
   ```

5. **Set `rabbit_queue_type: quorum`** in the config that every component reads.
6. **Start everything again.** Each queue is redeclared as quorum on first use. To
   check: `rabbitmqctl list_queues -p <vhost> name type`.

If a message was deleted without draining anyway, its execution stays SUBMITTED. The
outbox reconciler (`health.reconcile_lost_messages`) re-publishes it from the stored
copy if `message_outbox` is enabled. Otherwise it marks the execution ERROR once
`reconcile_lost_messages_after_s` has passed.

To roll back, run the same steps with the flag set back to `classic`.
