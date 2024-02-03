from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import Generator, Optional

import pytest
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from statemachine import State, StateMachine

from snailqueue import Queue, SqlAlchemyConnector, TaskIdLogic

meta = MetaData()


@dataclass
class Message(object):
    message_id: Optional["uuid.UUID"] = None

    def db_serialize(self: "Message") -> dict[str, str]:  # noqa: PLR6301
        return {}

    @staticmethod
    def db_deserialize(row: dict[str, str]) -> "Message":
        return Message(message_id=row["id"])


class MessageQueue(Queue[str, Message]):
    pass


@pytest.fixture()
def database_url() -> str:
    return "postgresql+asyncpg://postgres:postgres@localhost:5432/postgres"


@pytest.fixture()
async def engine(database_url: str) -> Generator[AsyncEngine, None, None]:
    engine = create_async_engine(
        database_url,
        echo=False,
    )
    async with engine.begin() as conn:
        await conn.run_sync(meta.drop_all)
        await conn.execute(text('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"'))
        await conn.run_sync(meta.create_all)

    yield engine

    await engine.dispose()


message_table = Table(
    "message",
    meta,
    Column(
        "id",
        UUID,
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    ),
    Column("locked_by_name", String),
    Column("locked_by_time", DateTime(timezone=True)),
    Column("time_created", DateTime(timezone=True), server_default=func.now()),
)


# https://github.com/fgmacedo/python-statemachine
class TaskStateMachine(StateMachine):
    enqueued = State(initial=True)
    processing = State()
    completed = State(final=True)

    switch_to_processing = enqueued.to(processing)
    switch_to_completed = processing.to(completed)

    def __init__(self) -> None:
        super().__init__()
        self.timeouted = 0


@dataclass
class Task:
    state: TaskStateMachine
    parse_id: "uuid.UUID"
    id: Optional[str] = None

    def db_serialize(self: "Task") -> dict[str, str]:
        return {
            "state": self.state.current_state.id,
            "parse_id": str(self.parse_id),
        }

    @staticmethod
    def db_deserialize(row: dict[str, str]) -> "Task":
        return Task(
            id=row["id"],
            state=row["state"],
            parse_id=row["parse_id"],
        )


class TaskQueue(Queue[str, Task]):
    pass


task_table = Table(
    "task",
    meta,
    Column(
        "id",
        UUID,
        primary_key=True,
        server_default=text("uuid_generate_v4()"),
    ),
    Column("parse_id", String, nullable=False),
    Column("state", String, nullable=False),
    Column("attempts", Integer, default=0),
    Column("locked_by_name", String),
    Column("locked_by_time", DateTime(timezone=True)),
    Column("time_created", DateTime(timezone=True), server_default=func.now()),
)


@pytest.mark.fast()
@pytest.mark.asyncio()
@pytest.mark.require_db()
async def test_state(engine: AsyncEngine) -> None:
    connector = SqlAlchemyConnector(
        engine=engine,
        table=task_table,
        id_logic=TaskIdLogic(task_table.c.id),
        # priority_logic=TaskPriorityLogic(task_table.c.time_created),
        #         # lock_logic=TaskLockByTimeLogic(task_table.c.locked_by_time, name_column=task_table.c.locked_by_name, seconds=0.1),
        #         # attempts_logic=TaskAttemptsLogic(task_table.c.attempts, max_attempts=5),
        #         # codec=TaskCodec(Task.db_serialize, Task.db_deserialize),
        #         # states=TaskStates(task_table.c.state, init=["enqueued"]),
        id_column=task_table.c.id,
        priority_column=task_table.c.time_created,
        locked_by_name_column=task_table.c.locked_by_name,
        locked_by_time_column=task_table.c.locked_by_time,
        serializer=Task.db_serialize,
        deserializer=Task.db_deserialize,
        init_states=["enqueued"],
        state_column=task_table.c.state,
        attempts_column=task_table.c.attempts,
        timeout_seconds=60,
    )
    q = TaskQueue(connector=connector)
    task = Task(state=TaskStateMachine(), parse_id=uuid.uuid4())
    task_id = await q.put(task)

    read_msg = await q.pull({"state": "processing"})
    assert read_msg

    assert read_msg.parse_id == str(task.parse_id)

    # Nothing to pull
    assert await q.pull({"state": "processing"}) is None

    task = await q.get_task(task_id)
    assert task.state == "processing"

    task = await q.patch_task(task_id, {"state": "completed"})
    assert task.state == "completed"

    # Nothing to pull
    assert await q.pull({"state": "processing"}) is None


@pytest.mark.fast()
@pytest.mark.asyncio()
@pytest.mark.require_db()
async def test_retries(engine: AsyncEngine) -> None:
    connector = SqlAlchemyConnector(
        engine=engine,
        table=task_table,
        id_logic=TaskIdLogic(task_table.c.id),
        id_column=task_table.c.id,
        priority_column=task_table.c.time_created,
        locked_by_name_column=task_table.c.locked_by_name,
        locked_by_time_column=task_table.c.locked_by_time,
        serializer=Task.db_serialize,
        deserializer=Task.db_deserialize,
        init_states=["enqueued"],
        state_column=task_table.c.state,
        attempts_column=task_table.c.attempts,
        timeout_seconds=None,
        max_attempts=5,
    )
    q = TaskQueue(connector=connector)
    task = Task(state=TaskStateMachine(), parse_id=uuid.uuid4())
    await q.put(task)

    read_msg = await q.pull({"state": "processing"})
    assert read_msg

    assert read_msg.parse_id == str(task.parse_id)

    read_msg_2 = await q.pull({"state": "processing"})
    assert read_msg_2

    read_msg_3 = await q.pull({"state": "processing"})
    assert read_msg_3


@pytest.mark.fast()
@pytest.mark.asyncio()
@pytest.mark.require_db()
async def test_retries_sleep(engine: AsyncEngine) -> None:
    connector = SqlAlchemyConnector(
        engine=engine,
        table=task_table,
        id_logic=TaskIdLogic(task_table.c.id),
        id_column=task_table.c.id,
        priority_column=task_table.c.time_created,
        locked_by_name_column=task_table.c.locked_by_name,
        locked_by_time_column=task_table.c.locked_by_time,
        serializer=Task.db_serialize,
        deserializer=Task.db_deserialize,
        init_states=["enqueued"],
        state_column=task_table.c.state,
        attempts_column=task_table.c.attempts,
        timeout_seconds=0.1,
        max_attempts=5,
    )
    q = TaskQueue(connector=connector)
    task = Task(state=TaskStateMachine(), parse_id=uuid.uuid4())
    await q.put(task)

    read_msg = await q.pull({"state": "processing"})
    assert read_msg

    assert read_msg.parse_id == str(task.parse_id)

    await asyncio.sleep(0.5)

    read_msg_2 = await q.pull({"state": "processing"})
    assert read_msg_2

    await asyncio.sleep(0.5)

    read_msg_3 = await q.pull({"state": "processing"})
    assert read_msg_3


async def long_poll(q: TaskQueue) -> int:
    processed_tasks = 0
    while True:
        task = await q.pull({"state": "processing"})
        if not task:
            break
        await q.patch_task(task.id, {"state": "completed"})
        processed_tasks += 1
    return processed_tasks


@pytest.mark.asyncio()
@pytest.mark.benchmark()
@pytest.mark.require_db()
async def test_benchmark(engine: AsyncEngine) -> None:
    connector = SqlAlchemyConnector(
        engine=engine,
        table=task_table,
        id_column=task_table.c.id,
        priority_column=task_table.c.time_created,
        locked_by_name_column=task_table.c.locked_by_name,
        locked_by_time_column=task_table.c.locked_by_time,
        serializer=Task.db_serialize,
        deserializer=Task.db_deserialize,
        init_states=["enqueued"],
        state_column=task_table.c.state,
        attempts_column=task_table.c.attempts,
        max_attempts=3,
        timeout_seconds=10.0,
    )
    q = TaskQueue(connector=connector)
    for _ in range(500):
        task = Task(state=TaskStateMachine(), parse_id=uuid.uuid4())
        await q.put(task)

    for _ in range(100):
        task = await q.pull(state="processing")
        await q.patch_task(task.id, {"state": "completed"})

    iterations = 1
    processed_tasks = 0
    total_time = 0.0
    for _ in range(iterations):
        start_time = time.perf_counter()
        processed_tasks += await long_poll(q=q)
        end_time = time.perf_counter()
        total_time += end_time - start_time
    avg_time_per_iteration = total_time / iterations
    print(f"Avg time per iteration: {avg_time_per_iteration:.6f} seconds")

    tps = float(processed_tasks) / (total_time)

    print(f"Time {total_time:.1f}, {processed_tasks=}, {tps=}")
    assert False


# @pytest.mark.fast()
# @pytest.mark.asyncio()
# @pytest.mark.require_db()
# async def test_interface(engine: AsyncEngine) -> None:
#     connector = SqlAlchemyConnector(
#         engine=engine,
#         table=task_table,
#         id_logic=TaskIdLogic(task_table.c.id),
#         # priority_logic=TaskPriorityLogic(task_table.c.time_created),
#         # lock_logic=TaskLockByTimeLogic(task_table.c.locked_by_time, name_column=task_table.c.locked_by_name, seconds=0.1),
#         # attempts_logic=TaskAttemptsLogic(task_table.c.attempts, max_attempts=5),
#         # codec=TaskCodec(Task.db_serialize, Task.db_deserialize),
#         # states=TaskStates(task_table.c.state, init=["enqueued"]),
#         id_column=task_table.c.id,
#         priority_column=task_table.c.time_created,
#         locked_by_name_column=task_table.c.locked_by_name,
#         locked_by_time_column=task_table.c.locked_by_time,
#         serializer=Task.db_serialize,
#         deserializer=Task.db_deserialize,
#         init_states=["enqueued"],
#         state_column=task_table.c.state,
#         attempts_column=task_table.c.attempts,
#         max_attempts=3,
#         timeout_seconds=10.0,
#     )
#     q = TaskQueue(connector=connector)
#     task = Task(state=TaskStateMachine(), parse_id=uuid.uuid4())
#     await q.put(task)

#     read_msg = await q.pull({"state": "processing"})
#     assert read_msg

#     assert read_msg.parse_id == str(task.parse_id)

#     await asyncio.sleep(0.5)

#     read_msg_2 = await q.pull({"state": "processing"})
#     assert read_msg_2

#     await asyncio.sleep(0.5)

#     read_msg_3 = await q.pull({"state": "processing"})
#     assert read_msg_3
