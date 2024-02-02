from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator, Callable, Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

if TYPE_CHECKING:
    from sqlalchemy import Column, Table
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from snailqueue.base import Connector, MessageType

SerializerCallable = Callable[[MessageType], dict[str, Any]]
DeserializerCallable = Callable[[dict[str, Any]], MessageType]


class SqlAlchemyConnector(Connector):
    def __init__(  # noqa: PLR0913
        self,
        *,
        engine: "AsyncEngine",
        table: "Table",
        id_column: "Column",
        priority_column: "Column",
        locked_by_time_column: "Column",
        locked_by_name_column: "Column",
        serializer: SerializerCallable,
        deserializer: DeserializerCallable,
    ) -> None:
        self._engine = engine
        self._table = table
        self._id_column = id_column
        self._priority_column = priority_column
        self._locked_by_time_column = locked_by_time_column
        self._locked_by_name_column = locked_by_name_column
        self._serializer = serializer
        self._deserializer = deserializer

    async def put(self, queue_name: str, message: MessageType) -> None:  # noqa: ARG002
        async with self._begin() as conn:
            query = (
                insert(self._table)
                .values(
                    self._serializer(message),
                )
                .returning(self._id_column)
            )

            res = await conn.execute(query)
            raw = res.one_or_none()
            if not raw:
                return None
            return raw._asdict()["id"]

    async def pull(self, queue_name: str) -> Optional[MessageType]:  # noqa: ARG002
        async with self._begin() as conn:
            query = (
                select(self._table)
                .where(
                    self._locked_by_time_column.is_(None)
                    | (self._locked_by_time_column.is_(None)),
                )
                .order_by(self._priority_column)
                .limit(1)
            )

            res = await conn.execute(query)
            raw = res.one_or_none()
            if not raw:
                return None
            return self._deserializer(raw._asdict())

    @asynccontextmanager
    async def _connect(self) -> AsyncGenerator["AsyncConnection", None]:
        """Make connection. Don't forget to .commit() changes."""
        async with self._engine.connect() as conn:
            yield conn

    @asynccontextmanager
    async def _begin(self) -> AsyncGenerator["AsyncConnection", None]:
        """Begin transaction."""
        async with self._engine.begin() as conn:
            yield conn
