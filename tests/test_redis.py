"""Two Hubs on one Redis, which is what "across processes" actually means.

Everything in test_hub.py is true of a single Hub talking to itself. None of it proves the
part people install this for. These use two independent Hubs on the same Redis, which is
the same arrangement as two application processes behind a load balancer -- one holds the
connection that publishes, the other holds the connection that is subscribed.

Against a real Redis, because the promise is about what Redis does with pub/sub while
subscribers come and go, and a fake would only prove the fake behaves.
"""

import asyncio
import os

import pytest

from hubcast import Hub, Overflow, SubscriberTooSlow
from hubcast._hub import KEEPALIVE

redis_asyncio = pytest.importorskip("redis.asyncio")

TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/15")


async def topic_channels(redis, prefix="test"):
    """Channels the Hub holds for topics, without the keepalive.

    Every Hub keeps one channel open for its whole life so redis-py's listen() has
    something to loop on -- see hubcast._hub.KEEPALIVE. It is expected, so it is filtered
    here rather than asserted around.
    """
    channels = await redis.pubsub_channels(f"{prefix}:*")
    return sorted(c for c in channels if c != f"{prefix}:{KEEPALIVE}")


async def collect(subscription, count, timeout=5.0):
    async def read():
        messages = []
        async for message in subscription:
            messages.append(message)
            if len(messages) == count:
                return messages
        return messages

    return await asyncio.wait_for(read(), timeout)


@pytest.fixture
async def redis():
    client = redis_asyncio.Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    await client.flushdb()
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()


@pytest.fixture
async def two_hubs(redis):
    """Two Hubs sharing one Redis: as far as either is concerned, the other is elsewhere."""
    publisher = Hub(redis, prefix="test")
    subscriber = Hub(redis, prefix="test")
    await publisher.start()
    await subscriber.start()
    try:
        yield publisher, subscriber
    finally:
        await publisher.stop()
        await subscriber.stop()


# -- the point of the package --------------------------------------------------------


async def test_a_message_published_on_one_hub_reaches_the_other(two_hubs):
    publisher, subscriber = two_hubs

    async with subscriber.subscribe("links") as messages:
        await asyncio.sleep(0.1)  # let the subscribe reach Redis
        await publisher.publish("links", "from the other process")

        assert await collect(messages, 1) == ["from the other process"]


async def test_every_instance_with_a_subscriber_receives_it(redis):
    """Three processes, one publish, three deliveries."""
    hubs = [Hub(redis, prefix="test") for _ in range(3)]
    for hub in hubs:
        await hub.start()
    try:
        async with hubs[0].subscribe("links") as a, hubs[1].subscribe("links") as b:
            async with hubs[2].subscribe("links") as c:
                await asyncio.sleep(0.1)
                await hubs[0].publish("links", "one")

                for messages in (a, b, c):
                    assert await collect(messages, 1) == ["one"]
    finally:
        for hub in hubs:
            await hub.stop()


async def test_a_publisher_needs_no_local_subscriber(two_hubs):
    """The ordinary shape: the process recording an event has nobody watching it."""
    publisher, subscriber = two_hubs

    async with subscriber.subscribe("links") as messages:
        await asyncio.sleep(0.1)
        await publisher.publish("links", "one")

        assert await collect(messages, 1) == ["one"]
        assert publisher.subscribers("links") == 0


async def test_order_survives_the_round_trip(two_hubs):
    publisher, subscriber = two_hubs

    async with subscriber.subscribe("links") as messages:
        await asyncio.sleep(0.1)
        for number in range(10):
            await publisher.publish("links", str(number))

        assert await collect(messages, 10) == [str(n) for n in range(10)]


async def test_a_topic_nobody_subscribes_to_is_not_delivered(two_hubs):
    publisher, subscriber = two_hubs

    async with subscriber.subscribe("links") as messages:
        await asyncio.sleep(0.1)
        await publisher.publish("users", "not for you")
        await publisher.publish("links", "for you")

        assert await collect(messages, 1) == ["for you"]


# -- one connection, whatever the number of subscribers ------------------------------


async def test_a_hub_holds_one_redis_connection_however_many_subscribers(redis):
    """The reason this package exists.

    Giving every connection its own Redis subscription works until the scale where
    broadcasting was worth doing, and then runs the server out of file descriptors. Redis
    should see the number of topics a process cares about, not the number of clients it is
    serving.
    """
    before = len(await redis.client_list())

    hub = Hub(redis, prefix="test")
    await hub.start()
    try:
        async with (
            hub.subscribe("links"),
            hub.subscribe("links"),
            hub.subscribe("links"),
            hub.subscribe("links"),
            hub.subscribe("users"),
        ):
            await asyncio.sleep(0.2)
            after = len(await redis.client_list())

            # One connection for the listener. Five subscribers over two topics add none.
            assert after - before == 1
    finally:
        await hub.stop()


async def test_redis_is_told_about_a_topic_once_and_released_once(redis):
    hub = Hub(redis, prefix="test")
    await hub.start()
    try:
        async with hub.subscribe("links"):
            await asyncio.sleep(0.1)
            assert await topic_channels(redis) == ["test:links"]

            async with hub.subscribe("links"):
                await asyncio.sleep(0.1)
                # A second local subscriber is not a second Redis subscription.
                assert await topic_channels(redis) == ["test:links"]

            await asyncio.sleep(0.1)
            # Nor does the first one leaving take it away.
            assert await topic_channels(redis) == ["test:links"]

        await asyncio.sleep(0.2)
        assert await topic_channels(redis) == []
    finally:
        await hub.stop()


async def test_prefixes_keep_two_applications_apart(redis):
    one = Hub(redis, prefix="app-one")
    two = Hub(redis, prefix="app-two")
    await one.start()
    await two.start()
    try:
        async with one.subscribe("links") as mine, two.subscribe("links") as theirs:
            await asyncio.sleep(0.1)
            await one.publish("links", "mine")

            assert await collect(mine, 1) == ["mine"]
            with pytest.raises(asyncio.TimeoutError):
                await collect(theirs, 1, timeout=0.4)
    finally:
        await one.stop()
        await two.stop()


# -- the slow subscriber, across the wire --------------------------------------------


async def test_overflow_still_applies_to_messages_from_elsewhere(two_hubs):
    """The queue is local, so the policy has to hold for messages that arrived over
    Redis exactly as it does for local ones."""
    publisher, subscriber = two_hubs

    async with subscriber.subscribe("links", max_queue=3) as messages:
        await asyncio.sleep(0.1)
        for number in range(9):
            await publisher.publish("links", str(number))
        await asyncio.sleep(0.3)

        assert await collect(messages, 3) == ["6", "7", "8"]
        assert messages.dropped == 6


async def test_a_slow_subscriber_does_not_stall_the_listener(two_hubs):
    """One full queue must not stop the process delivering to everyone else -- the
    listener is shared, so blocking on one subscriber would block them all."""
    publisher, subscriber = two_hubs

    async with (
        subscriber.subscribe("links", max_queue=1, overflow=Overflow.CLOSE) as stuck,
        subscriber.subscribe("links", max_queue=100) as healthy,
    ):
        await asyncio.sleep(0.1)
        for number in range(20):
            await publisher.publish("links", str(number))
        await asyncio.sleep(0.4)

        assert await collect(healthy, 20) == [str(n) for n in range(20)]
        with pytest.raises(SubscriberTooSlow):
            await collect(stuck, 20, timeout=1.0)


async def test_a_hub_that_stops_leaves_no_subscription_behind(redis):
    hub = Hub(redis, prefix="test")
    await hub.start()
    subscribe = hub.subscribe("links")
    await subscribe.__aenter__()
    await asyncio.sleep(0.1)
    assert await topic_channels(redis) == ["test:links"]

    await hub.stop()
    await asyncio.sleep(0.3)

    # Not even the keepalive: stopping a Hub leaves nothing of it on the server.
    assert (await redis.pubsub_channels("test:*")) == []


async def test_a_client_that_returns_bytes_works_too():
    """Redis.from_url() without decode_responses=True is the common way to build a client,
    and it hands back bytes. A hub that only worked with the other spelling would be
    broken for most of the people who install it -- and would look fine in a test suite
    that always passes decode_responses."""
    client = redis_asyncio.Redis.from_url(TEST_REDIS_URL)  # no decode_responses
    await client.flushdb()
    hub = Hub(client, prefix="test")
    await hub.start()
    try:
        async with hub.subscribe("links") as messages:
            await asyncio.sleep(0.1)
            await hub.publish("links", "still a str on the way out")

            assert await collect(messages, 1) == ["still a str on the way out"]
    finally:
        await hub.stop()
        await client.flushdb()
        await client.aclose()


async def test_the_keepalive_channel_carries_nothing(redis):
    """Nothing publishes on it, but nothing stops anyone else from doing so by accident --
    and a stray message on it must not be handed to a subscriber as though it were data."""
    hub = Hub(redis, prefix="test")
    await hub.start()
    try:
        async with hub.subscribe("links") as messages:
            await asyncio.sleep(0.1)
            await redis.publish(f"test:{KEEPALIVE}", "not for anyone")
            await hub.publish("links", "for you")

            assert await collect(messages, 1) == ["for you"]
    finally:
        await hub.stop()
