"""Fan-out to WebSocket subscribers across processes, on one Redis connection.

The obvious implementation gives every connection its own Redis subscription, and stops
working at exactly the scale where broadcasting was worth doing. This keeps one connection
per process however many clients it is serving, and makes the slow-subscriber question --
which is not an edge case, it is a phone on a train -- something you answer on purpose.
"""

from hubcast._hub import (
    DEFAULT_MAX_QUEUE,
    DEFAULT_PREFIX,
    Hub,
    Overflow,
    SubscriberTooSlow,
    Subscription,
)

__all__ = [
    "DEFAULT_MAX_QUEUE",
    "DEFAULT_PREFIX",
    "Hub",
    "Overflow",
    "SubscriberTooSlow",
    "Subscription",
]

__version__ = "0.1.0"
