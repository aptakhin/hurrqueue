from __future__ import annotations

from collections import deque
from typing import Generic, Optional, Protocol, TypeVar

MessageType = TypeVar('MessageType')


class Connector(Protocol):
    async def put(self, queue_name: str, message: MessageType) -> None:
        ...

    async def pull(self, queue_name: str) -> Optional[MessageType]:
        ...


class InMemoryConnector(Connector):
    def __init__(self, storage: dict[str, deque]) -> None:
        self.storage = storage

    async def put(self, queue_name: str, message: MessageType) -> None:
        self.storage.setdefault(queue_name, deque())
        self.storage[queue_name].append(message)

    async def pull(self, queue_name: str) -> Optional[MessageType]:
        fetch = self.storage.get(queue_name)
        if not fetch:
            return None
        return fetch.popleft()


class Queue(Generic[MessageType]):
    def __init__(
        self,
        connector: Connector,
        name: str,
    ) -> None:
        self.connector = connector
        self.name = name

    async def put(self, message: MessageType) -> None:
        await self.connector.put(queue_name=self.name, message=message)

    async def pull(self) -> Optional[MessageType]:
        return await self.connector.pull(self.name)
