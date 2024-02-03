from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator, Callable, Optional

from sqlalchemy import and_, func, literal_column, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert

if TYPE_CHECKING:
    from sqlalchemy import Column, Table, Update
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from snailqueue.base import (
    Connector,
    IdType,
    StateType,
    TaskNotFoundError,
    TaskType,
)

SerializerCallable = Callable[[IdType], dict[str, Any]]
DeserializerCallable = Callable[[dict[str, Any]], IdType]


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
        init_states: list[StateType],
        state_column: "Column",
        attempts_column: "Column",
        timeout_seconds: Optional[float] = None,
    ) -> None:
        self._engine = engine
        self._table = table
        self._id_column = id_column
        self._priority_column = priority_column
        self._locked_by_time_column = locked_by_time_column
        self._locked_by_name_column = locked_by_name_column
        self._serializer = serializer
        self._deserializer = deserializer
        self._timeout_seconds = timeout_seconds
        self._state_column = state_column
        self._attempts_column = attempts_column
        self._init_states = init_states

    async def put(self, task: TaskType) -> IdType:
        async with self._begin() as conn:
            query = (
                insert(self._table)
                .values(
                    self._serializer(task),
                )
                .returning(self._id_column)
            )

            res = await conn.execute(query)
            raw = res.one()
            return raw._asdict()["id"]

    async def pull(
        self,
        state: Optional[StateType] = None,
        timeout_seconds: Optional[float] = None,
    ) -> Optional[TaskType]:
        async with self._begin() as conn:
            select_q = (
                select(self._id_column.label("fetch_id"))
                .where(
                    or_(
                        self._locked_by_time_column.is_(None),
                        and_(
                            self._state_column.in_(self._init_states),
                        ),
                        and_(
                            self._locked_by_time_column.isnot(None),
                            func.now() >= self._locked_by_time_column,
                        ),
                    ),
                )
                .order_by(self._priority_column)
                .limit(1)
                .subquery()
            )

            time_lock_text = "now()"
            if timeout_seconds is None:
                timeout_seconds = self._timeout_seconds
            if timeout_seconds is not None:
                time_lock_text += f" + interval '{timeout_seconds} seconds'"

            values = {
                self._locked_by_name_column.name: "xxx",
                self._attempts_column.name: text(
                    f"{self._attempts_column.name} + 1",
                ),
            }
            if state is not None:
                values["state"] = state
            if timeout_seconds:
                values[self._locked_by_time_column.name] = text(time_lock_text)

            query: "Update" = (
                update(self._table)
                .where(
                    self._id_column == select_q.c.fetch_id,
                )
                .values(
                    **values,
                )
                .returning(
                    literal_column("*"),
                )
            )

            res = await conn.execute(query)
            raw = res.one_or_none()
            if not raw:
                return None
            return self._deserializer(raw._asdict())

    async def get_task(self, task_id: IdType) -> TaskType:
        async with self._begin() as conn:
            select_q = select(self._table).where(
                self._id_column == task_id,
            )

            res = await conn.execute(select_q)
            raw = res.one_or_none()
            if not raw:
                raise TaskNotFoundError(task_id)
            return self._deserializer(raw._asdict())

    async def update_task(
        self,
        task_id: IdType,
        state: Optional[str] = None,
    ) -> TaskType:
        async with self._begin() as conn:
            values = {}
            if state is not None:
                values["state"] = state

            query: "Update" = (
                update(self._table)
                .where(
                    self._id_column == task_id,
                )
                .values(
                    **values,
                )
                .returning(
                    literal_column("*"),
                )
            )

            res = await conn.execute(query)
            raw = res.one_or_none()
            if not raw:
                raise TaskNotFoundError(task_id)
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
