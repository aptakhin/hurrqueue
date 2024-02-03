import uuid
from dataclasses import dataclass

from snailqueue import Queue


@dataclass
class Message(object):
    message_id: uuid.UUID


class MessageQueue(Queue[str, Message]):
    pass
