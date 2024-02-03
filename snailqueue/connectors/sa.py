from __future__ import annotations

from contextlib import asynccontextmanager
import copy
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Callable,
    Optional,
    Union,
)

from sqlalchemy import (
    TextClause,
    and_,
    func,
    literal_column,
    or_,
    select,
    text,
    update,
)
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

SerializerCallable = Callable[[TaskType], dict[str, Any]]
DeserializerCallable = Callable[[dict[str, Any]], IdType]


def _execute_chain(chain: list) -> Any:
    cur = None
    for it in chain:
        if cur is None:
            cur = it
        else:
            cur = it(cur)
    return cur


class SqlAlchemyConnector(Connector):
    def __init__(  # noqa: PLR0913
        self,
        *,
        engine: "AsyncEngine",
        table: "Table",
        id_logic: "TaskIdLogic",
        # priority_logic=TaskPriorityLogic(task_table.c.time_created),
        # lock_logic=TaskLockByTimeLogic(task_table.c.locked_by_time, name_column=task_table.c.locked_by_name, seconds=0.1),
        # attempts_logic=TaskAttemptsLogic(task_table.c.attempts, max_attempts=5),
        # codec=TaskCodec(Task.db_serialize, Task.db_deserialize),
        # states=TaskStates(task_table.c.state, init=["enqueued"])
        id_column: "Column",
        priority_column: "Column",
        locked_by_time_column: "Column",
        locked_by_name_column: "Column",
        serializer: SerializerCallable,
        deserializer: DeserializerCallable,
        init_states: list[StateType],
        state_column: "Column",
        attempts_column: "Column",
        locked_by: Optional[str] = None,
        max_attempts: Optional[int] = 1,
        timeout_seconds: Optional[float] = None,
    ) -> None:
        self._engine = engine
        self._table = table
        self._id_logic = id_logic
        # self._priority_logic=TaskPriorityLogic(task_table.c.time_created),
        # self._lock_logic=TaskLockByTimeLogic(task_table.c.locked_by_time, name_column=task_table.c.locked_by_name, seconds=0.1),
        # self._attempts_logic=TaskAttemptsLogic(task_table.c.attempts, max_attempts=5),
        # self._codec=TaskCodec(Task.db_serialize, Task.db_deserialize),
        # self._states=TaskStates(task_table.c.state, init=["enqueued"])

        self._locked_by = locked_by
        # self._id_column = id_column
        self._priority_column = priority_column
        self._locked_by_time_column = locked_by_time_column
        self._locked_by_name_column = locked_by_name_column
        self._serializer = serializer
        self._deserializer = deserializer
        self._timeout_seconds = timeout_seconds
        self._state_column = state_column
        self._attempts_column = attempts_column
        self._max_attempts = max_attempts
        self._init_states = init_states

    async def put(self, task: TaskType) -> IdType:
        async with self._begin() as conn:
            query = (
                insert(self._table)
                .values(
                    self._serializer(task),
                )
                .returning(self._id_logic.place())
            )

            res = await conn.execute(query)
            raw = res.one()
            return raw._asdict()[self._id_logic.place().name]

    async def pull(
        self,
        patch_values: dict[str, str],
        locked_by: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
    ) -> Optional[TaskType]:
        async with self._begin() as conn:
            select_q = (
                select(self._id_logic.place().label("fetch_id"))
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
                    self._attempts_column < self._max_attempts,
                )
                .order_by(self._priority_column)
                .limit(1)
                .subquery()
            )

            values: dict[str, Union[str, TextClause]] = {}

            values[self._attempts_column.name] = text(
                f"{self._attempts_column.name} + 1",
            )

            if not locked_by:
                locked_by = self._locked_by
            if locked_by:
                values[self._locked_by_name_column.name] = locked_by

            if timeout_seconds is None:
                timeout_seconds = self._timeout_seconds
            if timeout_seconds is not None:
                time_lock_text = "now()"
                time_lock_text += f" + interval '{timeout_seconds} seconds'"
                values[self._locked_by_time_column.name] = text(time_lock_text)

            values.update(patch_values)

            update_chain = [
                update(self._table),
                lambda u: self._id_logic.inject_where(u, select_q.c.fetch_id),
                lambda u: u.values(
                    **values,
                ),
                lambda u: u.returning(
                    literal_column("*"),
                ),
            ]
            query = _execute_chain(update_chain)

            res = await conn.execute(query)
            raw = res.one_or_none()
            if not raw:
                return None
            return self._deserializer(raw._asdict())

    async def get_task(self, task_id: IdType) -> TaskType:
        async with self._begin() as conn:
            select_q = select(self._table).where(
                self._id_logic.place() == task_id,
            )

            res = await conn.execute(select_q)
            raw = res.one_or_none()
            if not raw:
                raise TaskNotFoundError(task_id)
            return self._deserializer(raw._asdict())

    async def patch_task(
        self,
        task_id: IdType,
        values: dict[str, str],
    ) -> TaskType:
        async with self._begin() as conn:
            chain = [
                update(self._table),
                lambda u: self._id_logic.inject_where(u, task_id),
                lambda u: u.values(
                    **values,
                ),
                lambda u: u.returning(
                    literal_column("*"),
                ),
            ]
            query = _execute_chain(chain)

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


class TaskIdLogic:
    def __init__(self, id_columns: Union["Column", list["Column"]]) -> None:
        if isinstance(id_columns, list) and len(id_columns) > 1:
            raise ValueError(
                "More than one column in id is not supported yet!"
            )

        if isinstance(id_columns, list):
            self.id_column = id_columns[0]
        else:
            self.id_column = id_columns

    def place(self) -> Any:
        return self.id_column

    def inject_where(self, cte, value) -> Any:
        return cte.where(
            self.id_column == value,
        )
