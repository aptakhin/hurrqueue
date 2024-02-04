from __future__ import annotations

from contextlib import asynccontextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Callable,
    Generic,
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
    from sqlalchemy import Column, Table
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
    from sqlalchemy.sql.dml import DMLWhereBase

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
        priority_logic: "TaskPriorityLogic",
        lock_logic: "TaskLockByTimeLogic",
        attempts_logic: "TaskAttemptsLogic",
        codec: "TaskCodec",
        states: "TaskStates",
    ) -> None:
        self._engine = engine
        self._table = table
        self._id_logic = id_logic
        self._priority_logic = priority_logic
        self._lock_logic = lock_logic
        self._attempts_logic = attempts_logic
        self._codec = codec
        self._states = states

    async def put(self, task: TaskType) -> IdType:
        async with self._begin() as conn:
            query = (
                insert(self._table)
                .values(
                    self._codec.serialize(task),
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
            select_chain = [
                select(self._id_logic.place().label("fetch_id")),
                lambda u: u.where(
                    or_(
                        and_(
                            self._lock_logic.inject_where_unlocked(),
                            self._states.inject_where_init_states(),
                        ),
                        and_(
                            self._states.inject_where_not_init_states(),
                            or_(
                                self._lock_logic.inject_where_unlocked(),
                                self._lock_logic.inject_where_was_locked(),
                            ),
                        ),
                    ),
                    self._attempts_logic.inject_condition(),
                ),
                lambda u: self._priority_logic.inject_order_by(u),
                lambda u: u.limit(1),
                lambda u: u.subquery(),
            ]
            select_q = _execute_chain(select_chain)

            values: dict[str, Union[str, TextClause]] = {}

            if attempts_values := self._attempts_logic.get_attempts_values():
                values.update(attempts_values)

            if locked_by_values := self._lock_logic.get_locked_by_values(
                locked_by
            ):
                values.update(locked_by_values)

            if timeout_values := self._lock_logic.get_timeout_values(
                timeout_seconds
            ):
                values.update(timeout_values)

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
            return self._codec.deserialize(raw._asdict())

    async def get_task(self, task_id: IdType) -> TaskType:
        async with self._begin() as conn:
            select_q = select(self._table).where(
                self._id_logic.place() == task_id,
            )

            res = await conn.execute(select_q)
            raw = res.one_or_none()
            if not raw:
                raise TaskNotFoundError(task_id)
            return self._codec.deserialize(raw._asdict())

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
            return self._codec.deserialize(raw._asdict())

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
                "More than one column in id is not supported yet!",
            )

        if isinstance(id_columns, list):
            self.id_column = id_columns[0]
        else:
            self.id_column = id_columns

    def place(self) -> "Column":
        return self.id_column

    def inject_where(self, cte: "DMLWhereBase", value: Any) -> "DMLWhereBase":
        return cte.where(
            self.id_column == value,
        )


class TaskPriorityLogic:
    def __init__(
        self, priority_columns: Union["Column", list["Column"]]
    ) -> None:
        if isinstance(priority_columns, list) and len(priority_columns) > 1:
            raise ValueError(
                "More than one column in priority is not supported yet!",
            )

        if isinstance(priority_columns, list):
            self.priority_column = priority_columns[0]
        else:
            self.priority_column = priority_columns

    def place(self) -> "Column":
        return self.priority_column

    def inject_order_by(self, cte):
        return cte.order_by(
            self.priority_column,
        )


class TaskLockByTimeLogic:
    def __init__(
        self,
        time_column: "Column",
        name_column: Optional["Column"],
        seconds: float,
        name: Optional[str] = None,
    ) -> None:
        self.time_column = time_column
        self.name_column = name_column
        self.seconds = seconds
        self.name = name
        # task_table.c.locked_by_time, name_column=task_table.c.locked_by_name, seconds=60.)

    def inject_where_unlocked(self) -> "DMLWhereBase":
        return self.time_column.is_(None)

    def inject_where_was_locked(self) -> "DMLWhereBase":
        return and_(
            self.time_column.isnot(None),
            func.now() >= self.time_column,
        )

    def get_locked_by_values(self, name: Optional[str]):
        if self.name_column is None:
            return {}

        name = self.name or name
        return {
            self.name_column.name: name,
        }

    def get_timeout_values(self, seconds: Optional[float]):
        timeout_seconds = self.seconds or seconds
        if timeout_seconds is None:
            return {}

        time_lock_text = "now()"
        time_lock_text += f" + interval '{timeout_seconds} seconds'"
        return {
            self.time_column.name: text(time_lock_text),
        }


class TaskAttemptsLogic:
    def __init__(self, attempts_column: "Column", max_attempts: int = 1):
        self.attempts_column = attempts_column
        self.max_attempts = max_attempts

    def inject_condition(self):
        return self.attempts_column < self.max_attempts

    def get_attempts_values(self) -> dict[str, Union[TextClause, str]]:
        return {
            self.attempts_column.name: text(
                f"{self.attempts_column.name} + 1"
            ),
        }


class TaskCodec(Generic[TaskType]):
    def __init__(self, serializer, deserializer) -> None:
        self.serializer = serializer
        self.deserializer = deserializer

    def serialize(self, d) -> None:
        return self.serializer(d)

    def deserialize(self, d) -> None:
        return self.deserializer(d)


class TaskStates:
    def __init__(self, state_column: "Column", init_states: list[str]) -> None:
        self.state_column = state_column
        self.init_states = init_states

    def inject_where_init_states(self):
        return self.state_column.in_(self.init_states)

    def inject_where_not_init_states(self):
        return self.state_column.notin_(self.init_states)
