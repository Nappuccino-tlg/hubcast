"""The in-process behaviour: subscribing, fan-out, and what a slow subscriber costs.

No Redis here. Without one a Hub is a perfectly good local hub, and everything below is
true of both -- the Redis tests then check that the same promises survive a second
process.
"""

import asyncio

import pytest

from hubcast import Hub, Overflow, SubscriberTooSlow


async def collect(subscription, count, timeout=2.0):
    """The next `count` messages, or fail rather than hang the suite.

    zip() does not take async iterators, so this reads them one at a time.
    """

    async def read():
        messages = []
        async for message in subscription:
            messages.append(message)
            if len(messages) == count:
                return messages
        return messages

    return await asyncio.wait_for(read(), timeout)


# -- delivery ------------------------------------------------------------------------


async def test_a_subscriber_receives_what_is_published():
    async with Hub() as hub, hub.subscribe("links") as messages:
        await hub.publish("links", "one")
        assert await collect(messages, 1) == ["one"]


async def test_every_subscriber_to_a_topic_receives_it():
    async with Hub() as hub, hub.subscribe("links") as first, hub.subscribe("links") as second:
        await hub.publish("links", "one")

        assert await collect(first, 1) == ["one"]
        assert await collect(second, 1) == ["one"]


async def test_other_topics_are_not_disturbed():
    async with Hub() as hub, hub.subscribe("links") as links, hub.subscribe("users") as users:
        await hub.publish("links", "for links")

        assert await collect(links, 1) == ["for links"]
        assert users.dropped == 0
        with pytest.raises(asyncio.TimeoutError):
            await collect(users, 1, timeout=0.15)


async def test_order_is_preserved():
    async with Hub() as hub, hub.subscribe("links") as messages:
        for number in range(5):
            await hub.publish("links", str(number))

        assert await collect(messages, 5) == ["0", "1", "2", "3", "4"]


async def test_publishing_to_nobody_is_not_an_error():
    """The ordinary case for a live feed: a link nobody happens to be watching."""
    async with Hub() as hub:
        await hub.publish("links", "into the void")


# -- registry ------------------------------------------------------------------------


async def test_a_subscription_is_gone_once_its_block_ends():
    """The leak this context manager exists to prevent is silent and cumulative."""
    hub = Hub()
    async with hub:
        async with hub.subscribe("links"):
            assert hub.subscribers("links") == 1
        assert hub.subscribers("links") == 0
        assert hub.topics == set()


async def test_a_subscription_is_released_even_when_the_body_raises():
    hub = Hub()
    async with hub:
        with pytest.raises(ZeroDivisionError):
            async with hub.subscribe("links"):
                raise ZeroDivisionError("the connection dropped")

        assert hub.subscribers("links") == 0


async def test_stopping_the_hub_ends_every_subscription():
    hub = Hub()
    await hub.start()
    subscribe = hub.subscribe("links")
    messages = await subscribe.__aenter__()

    await hub.stop()

    with pytest.raises(StopAsyncIteration):
        await messages.__anext__()


async def test_a_topic_reports_its_local_subscribers():
    async with Hub() as hub:
        async with hub.subscribe("links"), hub.subscribe("links"), hub.subscribe("users"):
            assert hub.subscribers("links") == 2
            assert hub.subscribers("users") == 1
            assert hub.subscribers("nobody") == 0
            assert hub.topics == {"links", "users"}


# -- overflow ------------------------------------------------------------------------


async def test_drop_oldest_keeps_the_newest():
    """What a live feed wants: the current number is the true one, an old one is worse
    than nothing."""
    async with Hub(max_queue=3) as hub, hub.subscribe("links") as messages:
        for number in range(6):
            await hub.publish("links", str(number))

        assert await collect(messages, 3) == ["3", "4", "5"]
        assert messages.dropped == 3


async def test_drop_newest_keeps_the_first():
    """What an alert stream wants: the original cause, not the hundredth symptom."""
    async with Hub(max_queue=3, overflow=Overflow.DROP_NEWEST) as hub:
        async with hub.subscribe("links") as messages:
            for number in range(6):
                await hub.publish("links", str(number))

            assert await collect(messages, 3) == ["0", "1", "2"]
            assert messages.dropped == 3


async def test_close_stops_a_subscriber_that_cannot_keep_up():
    """For a consumer that would rather resynchronise than continue with a hole in it."""
    async with Hub(max_queue=2, overflow=Overflow.CLOSE) as hub:
        async with hub.subscribe("links") as messages:
            for number in range(5):
                await hub.publish("links", str(number))

            with pytest.raises(SubscriberTooSlow):
                await collect(messages, 5, timeout=1.0)


async def test_the_policy_can_be_set_per_subscription():
    async with Hub(max_queue=2) as hub:
        async with hub.subscribe("links", overflow=Overflow.DROP_NEWEST, max_queue=2) as strict:
            for number in range(4):
                await hub.publish("links", str(number))

            assert await collect(strict, 2) == ["0", "1"]


async def test_one_slow_subscriber_does_not_affect_another():
    """A broadcast must not fail because somebody's phone went into a tunnel."""
    async with (
        Hub() as hub,
        hub.subscribe("links", max_queue=2) as slow,
        hub.subscribe("links", max_queue=100) as fast,
    ):
        for number in range(10):
            await hub.publish("links", str(number))

        assert slow.dropped == 8
        assert fast.dropped == 0
        assert await collect(fast, 10) == [str(n) for n in range(10)]


async def test_publishing_never_waits_on_a_subscriber():
    """The property the whole design rests on: a publisher's cost does not depend on how
    many subscribers there are or how slow the worst one is."""
    async with Hub(max_queue=1) as hub:
        async with hub.subscribe("links"), hub.subscribe("links"), hub.subscribe("links"):
            await asyncio.wait_for(
                asyncio.gather(*(hub.publish("links", str(n)) for n in range(200))),
                timeout=2.0,
            )


# -- configuration -------------------------------------------------------------------


async def test_a_queue_must_be_able_to_hold_something():
    with pytest.raises(ValueError, match="max_queue"):
        Hub(max_queue=0)


async def test_start_and_stop_are_safe_to_repeat():
    hub = Hub()
    await hub.start()
    await hub.start()
    await hub.stop()
    await hub.stop()


async def test_delivering_to_a_closed_subscription_is_dropped_quietly():
    """A message can arrive between a connection going away and its subscription being
    removed. Nothing is waiting for it, and that is not an error."""
    async with Hub() as hub:
        subscribe = hub.subscribe("links")
        messages = await subscribe.__aenter__()
        messages.close()

        await hub.publish("links", "too late")

        assert messages.dropped == 0


async def test_leaving_a_subscription_after_the_hub_stopped_is_not_an_error():
    """Shutdown order is not something a connection handler gets to control."""
    hub = Hub()
    await hub.start()
    subscribe = hub.subscribe("links")
    await subscribe.__aenter__()

    await hub.stop()
    await subscribe.__aexit__(None, None, None)
