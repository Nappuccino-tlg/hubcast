"""Fan-out to subscribers, across processes, without a Redis connection per subscriber.

The obvious way to broadcast to WebSocket clients spread over several application
processes is to give every connection its own Redis subscription. It works, and it stops
working at exactly the scale where broadcasting was worth doing: ten thousand connections
become ten thousand Redis connections, and the server runs out of file descriptors long
before it runs out of anything interesting.

So a Hub holds one Redis connection however many subscribers it has. It subscribes to a
channel when a topic gains its first local subscriber, unsubscribes when it loses its
last, and fans out in-process on the way through. Redis sees the number of *topics* a
process cares about, not the number of clients it is serving.

The other half is what happens when a subscriber cannot keep up, which is not an edge
case -- it is a phone on a train. See Overflow.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import defaultdict
from enum import Enum
from types import TracebackType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from redis.asyncio import Redis

DEFAULT_PREFIX = "hubcast"
DEFAULT_MAX_QUEUE = 100

# A channel the Hub subscribes to for its whole life and never publishes on.
#
# redis-py's listen() loops `while self.subscribed`, so a pubsub with no channels ends the
# generator immediately -- the listener would exit before the first topic arrived and the
# Hub would be silently deaf. Holding one channel open keeps the connection established
# and the loop alive from start() to stop(). It shows up in PUBSUB CHANNELS, which is why
# it is named for what it is rather than hidden.
KEEPALIVE = "__hub__"


class Overflow(str, Enum):
    """What to do with a subscriber whose queue is full.

    There is no default that is right everywhere, which is why this is an argument rather
    than a decision made for you.
    """

    #: Throw away the oldest unread message. Right for a live feed, where the newest
    #: number is the true one and an old one is worse than nothing.
    DROP_OLDEST = "drop_oldest"
    #: Throw away the message being delivered. Right when the first N matter most -- an
    #: alert stream where the original cause is the useful part.
    DROP_NEWEST = "drop_newest"
    #: Stop the subscriber. Right when missing a message is not survivable and the
    #: consumer would rather reconnect and resynchronise than continue with a hole in it.
    CLOSE = "close"


class SubscriberTooSlow(Exception):
    """Raised in a subscriber's own iteration when Overflow.CLOSE fires.

    Raised at the reader rather than the publisher on purpose: one slow client is the slow
    client's problem, and a broadcast must not fail because somebody's phone went into a
    tunnel.
    """


class Subscription:
    """One subscriber's view of a topic. Iterate it to receive.

    Created by `Hub.subscribe`; not constructed directly.
    """

    def __init__(self, topic: str, max_queue: int, overflow: Overflow) -> None:
        self.topic = topic
        self.overflow = overflow
        #: Messages this subscription dropped because it could not keep up. Worth putting
        #: on a dashboard: a number that climbs is a consumer that needs to be faster, or
        #: a queue that needs to be deeper, and silence about it is how that goes unnoticed.
        self.dropped = 0
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=max_queue)
        self._closed = False
        self._too_slow = False

    def _deliver(self, message: str) -> None:
        """Non-blocking by construction. A publisher never waits on a subscriber."""
        if self._closed:
            return
        try:
            self._queue.put_nowait(message)
            return
        except asyncio.QueueFull:
            pass

        if self.overflow is Overflow.DROP_NEWEST:
            self.dropped += 1
        elif self.overflow is Overflow.DROP_OLDEST:
            with contextlib.suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()
            self.dropped += 1
            with contextlib.suppress(asyncio.QueueFull):
                self._queue.put_nowait(message)
        else:  # Overflow.CLOSE
            self._too_slow = True
            self._wake()

    def _wake(self) -> None:
        """Nudge a reader that is blocked on an empty queue so it notices the state change."""
        with contextlib.suppress(asyncio.QueueFull):
            self._queue.put_nowait("")

    def close(self) -> None:
        self._closed = True
        self._wake()

    def __aiter__(self) -> Subscription:
        return self

    async def __anext__(self) -> str:
        while True:
            if self._too_slow:
                raise SubscriberTooSlow(f"subscriber to {self.topic!r} fell too far behind")
            if self._closed and self._queue.empty():
                raise StopAsyncIteration
            message = await self._queue.get()
            if message == "" and (self._closed or self._too_slow):
                continue  # the nudge, not a message
            return message


class Hub:
    """Publish and subscribe by topic, across every process sharing one Redis.

    Without a Redis it is a perfectly good in-process hub, which is what the tests and
    single-process deployments use. The same code path either way, so nothing is only
    exercised in production.
    """

    def __init__(
        self,
        redis: Redis | None = None,
        *,
        prefix: str = DEFAULT_PREFIX,
        max_queue: int = DEFAULT_MAX_QUEUE,
        overflow: Overflow = Overflow.DROP_OLDEST,
    ) -> None:
        if max_queue < 1:
            raise ValueError("max_queue must be at least 1")

        self.redis = redis
        self.prefix = prefix
        self.max_queue = max_queue
        self.overflow = overflow

        self._topics: dict[str, set[Subscription]] = defaultdict(set)
        self._pubsub = None
        self._listener: asyncio.Task[None] | None = None
        self._ready = asyncio.Event()
        # One lock over the whole subscribe/unsubscribe dance: the decision "am I the
        # first subscriber to this topic" and the Redis call it triggers have to be one
        # step, or two connections arriving together both think they are second.
        self._lock = asyncio.Lock()

    # -- lifecycle -------------------------------------------------------------------

    async def start(self) -> None:
        """Open the Redis connection and begin listening. A no-op without a Redis."""
        if self.redis is None or self._listener is not None:
            return
        self._pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        await self._pubsub.subscribe(self._channel(KEEPALIVE))
        self._listener = asyncio.create_task(self._listen())
        await self._ready.wait()

    async def stop(self) -> None:
        """Close everything and stop every subscription."""
        if self._listener is not None:
            self._listener.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listener
            self._listener = None
        if self._pubsub is not None:
            with contextlib.suppress(Exception):
                await self._pubsub.unsubscribe()
            await self._pubsub.aclose()
            self._pubsub = None
        self._ready.clear()

        for subscribers in list(self._topics.values()):
            for subscription in list(subscribers):
                subscription.close()
        self._topics.clear()

    async def __aenter__(self) -> Hub:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()

    # -- the listener ----------------------------------------------------------------

    def _channel(self, topic: str) -> str:
        return f"{self.prefix}:{topic}"

    def _topic(self, channel: str) -> str:
        return channel[len(self.prefix) + 1 :]

    async def _listen(self) -> None:
        assert self._pubsub is not None
        self._ready.set()
        async for message in self._pubsub.listen():
            channel = message["channel"]
            if isinstance(channel, bytes):
                channel = channel.decode()
            topic = self._topic(channel)
            if topic == KEEPALIVE:
                continue
            data = message["data"]
            if isinstance(data, bytes):
                data = data.decode()
            self._fan_out(topic, data)

    def _fan_out(self, topic: str, message: str) -> None:
        for subscription in list(self._topics.get(topic, ())):
            subscription._deliver(message)

    # -- the api ---------------------------------------------------------------------

    def subscribe(
        self,
        topic: str,
        *,
        max_queue: int | None = None,
        overflow: Overflow | None = None,
    ) -> _Subscribe:
        """Subscribe to `topic` for the length of an `async with` block.

            async with hub.subscribe(f"link:{code}") as messages:
                async for message in messages:
                    await websocket.send_text(message)

        A context manager rather than a pair of calls, because the failure it prevents --
        a connection dropping and leaving its subscription behind forever -- is silent,
        cumulative, and only shows up as memory that never comes back.
        """
        return _Subscribe(
            self,
            topic,
            self.max_queue if max_queue is None else max_queue,
            self.overflow if overflow is None else overflow,
        )

    async def publish(self, topic: str, message: str) -> None:
        """Send `message` to every subscriber of `topic`, on every instance.

        With a Redis, even local subscribers are served by the round trip rather than
        delivered directly. One path means every subscriber sees the same ordering, and
        the behaviour under test is the behaviour in production.
        """
        if self.redis is None:
            self._fan_out(topic, message)
        else:
            await self.redis.publish(self._channel(topic), message)

    def subscribers(self, topic: str) -> int:
        """Local subscribers to a topic. Other instances have their own."""
        return len(self._topics.get(topic, ()))

    @property
    def topics(self) -> set[str]:
        """Topics with at least one local subscriber. Never includes the keepalive."""
        return {topic for topic, subs in self._topics.items() if subs}

    # -- registry --------------------------------------------------------------------

    async def _add(self, subscription: Subscription) -> None:
        async with self._lock:
            first = not self._topics[subscription.topic]
            self._topics[subscription.topic].add(subscription)
            if first and self._pubsub is not None:
                await self._pubsub.subscribe(self._channel(subscription.topic))

    async def _remove(self, subscription: Subscription) -> None:
        async with self._lock:
            subscribers = self._topics.get(subscription.topic)
            if subscribers is None:
                return
            subscribers.discard(subscription)
            subscription.close()
            if not subscribers:
                del self._topics[subscription.topic]
                if self._pubsub is not None:
                    await self._pubsub.unsubscribe(self._channel(subscription.topic))


class _Subscribe:
    """The async context manager returned by Hub.subscribe."""

    def __init__(self, hub: Hub, topic: str, max_queue: int, overflow: Overflow) -> None:
        self._hub = hub
        self._subscription = Subscription(topic, max_queue, overflow)

    async def __aenter__(self) -> Subscription:
        await self._hub._add(self._subscription)
        return self._subscription

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self._hub._remove(self._subscription)
