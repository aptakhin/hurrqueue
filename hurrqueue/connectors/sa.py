from __future__ import annotations

from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Optional

from sqlalchemy import Column, Connection, Table, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from hurrqueue.base import Connector, MessageType


class SqlAlchemyConnector(Connector):
    def __init__(
        self,
        *,
        engine: AsyncEngine,
        table: Table,
        priority_column: Column,
        locked_by_time_column: Column,
        locked_by_name_column: Column,
        deserializer,
    ) -> None:
        self._engine = engine
        self._table = table
        self._priority_column = priority_column
        self._locked_by_time_column = locked_by_time_column
        self._locked_by_name_column = locked_by_name_column
        self._serializer = None
        self._deserializer = deserializer

    async def put(self, queue_name: str, message: MessageType) -> None:
        async with self._begin() as conn:
            query = (
                insert(self._table)
                .values(
                    **message.db_serialize(),
                )
                .returning(self._table.c.id)
            )

            res = await conn.execute(query)
            x = res.one_or_none()
            return x._asdict()["id"]

    async def pull(self, queue_name: str) -> Optional[MessageType]:
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
            x = res.one_or_none()
            if not x:
                return None
            return self._deserializer(x._asdict())

    @asynccontextmanager
    async def _connect(self) -> AbstractAsyncContextManager[Connection]:
        """Make connection. Don't forget to .commit() changes."""
        async with self._engine.connect() as conn:
            yield conn

    @asynccontextmanager
    async def _begin(self) -> AbstractAsyncContextManager[Connection]:
        """Begin transaction."""
        async with self._engine.begin() as conn:
            yield conn
