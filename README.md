# hubcast

Fan-out to WebSocket subscribers across processes, on one Redis connection.

[![CI](https://github.com/Nappuccino-tlg/hubcast/actions/workflows/ci.yml/badge.svg)](https://github.com/Nappuccino-tlg/hubcast/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.10%20--%203.13-blue)
![License](https://img.shields.io/badge/license-MIT-green)

```bash
pip install "hubcast[redis]"
```

## Two problems, and the second one is the hard one

You have a live feed — clicks arriving, prices moving, a build log — and clients watching
it over WebSockets. Two things go wrong, and only the first is obvious.

**Your app runs on more than one process.** The event arrives at instance A. The client
watching for it is connected to instance B. A knows nothing about B.

The usual fix is Redis pub/sub, and the usual implementation gives every connection its
own subscription. It works, and it stops working at exactly the scale where broadcasting
was worth doing: ten thousand clients become ten thousand Redis connections, and the
server runs out of file descriptors long before it runs out of anything interesting.

A `Hub` holds **one** Redis connection however many subscribers it has. It subscribes to a
channel when a topic gains its first local subscriber, drops it when it loses its last, and
fans out in-process. Redis sees how many *topics* a process cares about, not how many
clients it is serving.

**Someone's phone goes into a tunnel.** Their connection is open, their socket buffer is
full, and they are not reading. Now what?

That is not an edge case, and there is no answer that is right everywhere — which is why
it is an argument rather than a decision made for you.

## Using it

```python
from redis.asyncio import Redis
from hubcast import Hub

hub = Hub(Redis.from_url("redis://localhost"))

# once, at startup
await hub.start()
```

Publish from anywhere, on any instance:

```python
await hub.publish(f"link:{code}", json.dumps({"clicks": total}))
```

Subscribe for the length of a connection:

```python
@app.websocket("/live/{code}")
async def live(websocket: WebSocket, code: str):
    await websocket.accept()
    async with hub.subscribe(f"link:{code}") as messages:
        async for message in messages:
            await websocket.send_text(message)
```

`subscribe` is a context manager rather than a pair of calls because the failure it
prevents — a connection dropping and leaving its subscription behind forever — is silent,
cumulative, and shows up only as memory that never comes back.

## When a subscriber falls behind

```python
hub = Hub(redis, max_queue=100, overflow=Overflow.DROP_OLDEST)
```

| | keeps | right for |
|---|---|---|
| `DROP_OLDEST` *(default)* | the newest | a live feed — the current number is the true one, and a stale one is worse than nothing |
| `DROP_NEWEST` | the first | an alert stream — the original cause matters more than the hundredth symptom |
| `CLOSE` | nothing | a consumer that would rather reconnect and resynchronise than carry on with a hole in it |

Set per Hub, or per subscription:

```python
async with hub.subscribe(topic, max_queue=1000, overflow=Overflow.CLOSE) as messages:
    ...
```

`CLOSE` raises `SubscriberTooSlow` **in that subscriber's own iteration** — never in the
publisher. One slow client is the slow client's problem, and a broadcast must not fail
because somebody went into a tunnel.

Every subscription counts what it discarded:

```python
messages.dropped  # worth putting on a dashboard
```

A number that climbs means a consumer that needs to be faster or a queue that needs to be
deeper. Not printing it is how that goes unnoticed for a month.

## Without a Redis

```python
hub = Hub()  # in-process, same API
```

Useful for a single-process deployment and for tests, and it is the same code path — so
nothing here is only exercised in production. `redis` is an optional extra, and CI checks
the package still imports and works with it uninstalled.

## What it does not do

**Delivery guarantees.** Redis pub/sub is fire-and-forget: a subscriber that is not
connected when a message is published does not get it later. Nothing here adds replay,
acknowledgement or ordering across a reconnect. If losing a message is not survivable, you
want a log — Redis Streams, Kafka — not this.

**WebSockets.** A Hub moves strings between processes; it never touches a socket. That
keeps it usable from anything, and means heartbeats, reconnects and framing stay with the
framework that already owns them.

**Presence.** Knowing *who* is connected across instances is a different problem with its
own failure mode — an instance that dies leaves its users looking online forever.

## One thing worth knowing

Every Hub keeps one extra channel open, `<prefix>:__hub__`, and never publishes on it.
redis-py's `listen()` loops `while self.subscribed`, so a pubsub with no channels ends the
generator immediately — the listener would exit before the first topic arrived and the Hub
would be silently deaf. It is visible in `PUBSUB CHANNELS`, so it is named for what it is
rather than hidden.

## Tests

```bash
docker run -d -p 6379:6379 redis:7-alpine
pytest
```

The Redis tests use **two Hubs on one Redis**, which is the same arrangement as two
application processes behind a load balancer — one holds the connection that publishes,
the other the connection that is subscribed. A single Hub talking to itself proves nothing
about the part people install this for.

## Requirements

Python 3.10 or newer. `redis>=5.0` only if you want the cross-process half.

## License

MIT
