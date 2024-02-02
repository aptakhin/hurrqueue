import uuid
from dataclasses import dataclass

import pytest

from hurrqueue import InMemoryConnector, Queue


@dataclass
class Message(object):
    message_id: uuid.UUID


class MessageQueue(Queue[Message]):
    pass


@pytest.mark.asyncio()
async def test_basic() -> None:
    q = MessageQueue(connector=InMemoryConnector(storage={}), name="inmemory")
    msg = Message(message_id=uuid.uuid4())
    await q.put(msg)

    read_msg = await q.pull()
    assert read_msg

    assert read_msg.message_id == msg.message_id


def test_usage_type() -> None:
    _ = Queue[Message](
        connector=InMemoryConnector(storage={}),
        name="inmemory",
    )
