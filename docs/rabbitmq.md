# RabbitMQ in Abaco

How Abaco uses its broker beyond the defaults: switching the must-not-lose queues to quorum,
how messages that can never be processed are dead-lettered, and how to work with the
dead-letter queue.

## Switching queues to quorum (`rabbit_queue_type: quorum`)

`rabbit_queue_type` (default `classic`) decides what type of queue Abaco declares for
the queues that must not lose a message:

- `actor_msg_<dbid>`: actor inboxes
- `command_channel_<name>`: spawner commands
- `events_channel_<name>`: the events agent
- `abaco_dlq`: the dead-letter queue, declared by the rabbit init in `stores.py`

Short-lived per-worker queues (`worker_*`, `spawner_worker_*`, `results_*`) stay classic.

A queue's type is fixed when the queue is first declared. If you flip the flag on a
broker that already has these queues, every redeclare fails with
`406 PRECONDITION_FAILED - inequivalent arg 'x-queue-type'`. In practice that means:

- every `POST /messages` to an existing actor returns 503, and its execution is marked
  ERROR;
- the spawner and events agent loop forever on reconnect;
- workers cannot subscribe;
- rabbit init logs `rabbit DLX setup step failed` for `abaco_dlq`.

The failing component logs a CRITICAL line that starts with `queue type mismatch:` and
points to this document. Abaco never deletes queues itself, so you have to run the
migration below.

### Prerequisites

- RabbitMQ 3.12 or newer (this repo pins 3.12.14), with the `quorum_queue` feature flag
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

   Look at `abaco_dlq` too. It also has to be recreated with the new type. Inspect,
   replay or export its dead letters first (see "Working with the dead-letter queue"),
   because deleting it discards them.
3. **Stop every Abaco component**: the API containers, spawner(s), events, health and the
   workers. Every one of them redeclares these queues on connect.
4. **Delete the classic queues.** They have no `x-expires`, so they never expire on
   their own:

   ```
   rabbitmqctl list_queues -p <vhost> name type \
     | awk '$2 == "classic" && $1 ~ /^(actor_msg_|command_channel_|events_channel_|abaco_dlq$)/ {print $1}' \
     | while read q; do rabbitmqctl delete_queue -p <vhost> "$q"; done
   ```

5. **Set `rabbit_queue_type: quorum`** in the config that every component reads.
6. **Start everything again.** Rabbit init recreates `abaco_dlq` and the policies, and
   each queue is redeclared as quorum on first use. To check:
   `rabbitmqctl list_queues -p <vhost> name type policy`. Actor inboxes should show
   `quorum` with the `abaco-actor-msg-quorum` policy.

If a message was deleted without draining anyway, its execution stays SUBMITTED. The
outbox reconciler (`health.reconcile_lost_messages`) re-publishes it from the stored
copy if `message_outbox` is enabled. Otherwise it marks the execution ERROR once
`reconcile_lost_messages_after_s` has passed.

To roll back, run the same steps with the flag set back to `classic`.

## Dead-lettering and the delivery limit

Some messages can never be processed: a payload that can't be decoded, or one that
crashes its worker on every delivery. Without dead-lettering, RabbitMQ hands such a
"poison" message out forever. Rabbit init (`stores.rabbit_initialization`) declares, per
vhost:

- `abaco_dlx`: a fanout exchange that receives dead-lettered messages;
- `abaco_dlq`: the dead-letter queue bound to it (quorum when `rabbit_queue_type: quorum`);
- three policies. RabbitMQ applies exactly one policy to a queue: the matching one with the
  highest priority. These never overlap: the two actor-inbox policies are scoped by queue
  type, and the third matches only `abaco_dlq`.

| policy                   | pattern         | applies to     | definition                                                                                          |
|--------------------------|-----------------|----------------|-----------------------------------------------------------------------------------------------------|
| `abaco-actor-msg`        | `^actor_msg_`   | classic queues | `dead-letter-exchange: abaco_dlx`                                                                   |
| `abaco-actor-msg-quorum` | `^actor_msg_`   | quorum queues  | the same, plus `delivery-limit: <rabbit_delivery_limit>` (default 10)                                |
| `abaco-dlq`              | `^abaco_dlq$`   | all queues     | `max-length: <rabbit_dlq_max_length>` (default 10000), `message-ttl: <rabbit_dlq_message_ttl_ms>` (default 14 days), `overflow: drop-head` |

**Do not add `dead-letter-strategy: at-least-once`.** On RabbitMQ 3.12.14 its dead-letter worker
crash-loops whenever a quorum inbox is deleted while it is still being created, which happens
routinely (an actor deleted while its first worker starts). Its supervisor then gives up, and the
broker refuses every new quorum `actor_msg_` queue (`541 INTERNAL_ERROR`) until it is restarted.
This was reproduced on 2026-11-12; with the default at-most-once strategy the same races are
harmless. If you see it (`rabbitmqctl eval 'whereis(rabbit_fifo_dlx_sup).'` prints `undefined`,
or `list_queues name state` shows inboxes `down`), check the policy and restart the broker.

The two actor-inbox policies are separate because RabbitMQ 3.12 does not apply a policy at
all to a queue whose type lacks one of its keys: with `delivery-limit` in a single policy,
classic inboxes would silently lose their dead-letter exchange. `apply-to: classic_queues`
and `apply-to: quorum_queues` need RabbitMQ 3.12 or newer.

The limits are in policies, not queue arguments, so they can be changed on a live broker:
change the config and re-run rabbit init (it re-declares the policies on every reg start),
or run `rabbitmqadmin declare policy` by hand.

### Classic queues (the default): no delivery limit

Classic queues don't count deliveries. A message reaches `abaco_dlq` only when a worker
rejects it explicitly without requeueing it, for example a message that can't be
decoded. A poison message that crashes or kills its worker is requeued and redelivered
forever, as it always has been. To get delivery limits, switch to quorum (above).

Dead-lettering is at-most-once for both queue types: a dead letter that `abaco_dlq` cannot take
(the queue is missing) is dropped. Its execution is then handled by the reconciler like any
other lost message (see "Working with the dead-letter queue").

### Quorum queues

When a message has been delivered more than `delivery-limit` times, it is dead-lettered.
Benign requeues count toward that limit too: connections dropped by a missed heartbeat or
`consumer_timeout`, and workers that die while holding a message. Workers give back the
messages they never started (shutting down, a stale actor revision during an update, a
container that failed to start) as fresh copies, which do not count. That is why the
default is 10 and not 3.

## Working with the dead-letter queue

Nothing consumes `abaco_dlq` automatically. Its `abaco-dlq` policy bounds it: once it holds
`rabbit_dlq_max_length` messages the oldest are dropped, and any dead letter older than
`rabbit_dlq_message_ttl_ms` expires. Alert when its depth is above 0: every message there is
an execution that never ran, or did not run to the end. What happens to its execution depends
on `message_outbox`:

- **outbox off:** the reconciler in `health` marks it ERROR after
  `reconcile_lost_messages_after_s` (900s), with a status message that mentions `abaco_dlq`.
- **outbox on:** the reconciler first re-publishes the stored copy, at most
  `reconcile_max_requeues` times (default 1), and marks it ERROR only if that copy is lost too.
  So the dead letter may already have been retried, and the execution may since have completed.

Each dead letter keeps RabbitMQ's `x-death` header: the source queue
(`actor_msg_<tenant>_<actor id>`), the reason (`rejected` for an undecodable message,
`delivery_limit` for a poison message on a quorum inbox), a count and a timestamp. The
body is the original pickled message, so its `_abaco_execution_id` identifies the
execution.

**Inspect** without removing (the messages go back to the queue). Use the admin account Abaco is
configured with (`admin_rabbitmq_user` / `admin_rabbitmq_pass`); a broker started with
`RABBITMQ_DEFAULT_USER` has no `guest` user:

```
rabbitmqadmin -H <host> -P 15672 -u <admin user> -p <admin pass> -V <vhost> \
  get queue=abaco_dlq count=10 ackmode=ack_requeue_true
```

**Replay**: usually ask the user to send a new message instead. Before replaying, check each
dead letter's execution (`GET /actors/<actor id>/executions/<execution id>`): replay only an
execution that is ERROR. One that is COMPLETE, RUNNING or SUBMITTED was already retried from the
outbox (see above), and replaying it runs it again. When a replay is right (the cause was fixed
and the execution should run as it was), move dead letters back to their source queue. This
uses publisher confirms, so a message is only removed from `abaco_dlq` once its actor queue
has it; it stops at the first dead letter whose queue no longer exists:

```
docker run --rm -e RABBIT_URI='amqp://<user>:<pass>@<host>:5672/<vhost>' \
  --entrypoint python3 abaco/core-v3:<tag> -c '
import os, pika
conn = pika.BlockingConnection(pika.URLParameters(os.environ["RABBIT_URI"]))
ch = conn.channel(); ch.confirm_delivery()
moved = 0
while True:
    m, props, body = ch.basic_get("abaco_dlq", auto_ack=False)
    if m is None:
        break
    queue = ((props.headers or {}).get("x-death") or [{}])[0].get("queue", "")
    if not queue.startswith("actor_msg_"):
        print("not an actor message; stopping:", queue); break
    headers = {k: v for k, v in (props.headers or {}).items() if not k.startswith("x-")}
    ch.basic_publish("", queue, body, pika.BasicProperties(delivery_mode=2, headers=headers or None), mandatory=True)
    ch.basic_ack(m.delivery_tag)
    moved += 1
print("replayed", moved); conn.close()'
```

**Discard** everything in it:

```
rabbitmqctl purge_queue -p <vhost> abaco_dlq
```
