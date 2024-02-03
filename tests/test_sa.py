from __future__ import annotations

import asyncio
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

from snailqueue import Queue, SqlAlchemyConnector

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
        echo=True,
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


@pytest.mark.skip()
@pytest.mark.asyncio()
@pytest.mark.require_db()
async def test_basic(engine: AsyncEngine) -> None:
    connector = SqlAlchemyConnector(
        engine=engine,
        table=message_table,
        id_column=message_table.c.id,
        priority_column=message_table.c.time_created,
        locked_by_name_column=message_table.c.locked_by_name,
        locked_by_time_column=message_table.c.locked_by_time,
        serializer=Message.db_serialize,
        deserializer=Message.db_deserialize,
    )
    q = MessageQueue(connector=connector)
    msg = Message()
    write_message_id = await q.put(msg)

    read_msg = await q.pull()
    assert read_msg

    assert read_msg.message_id == write_message_id


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

    def db_serialize(self: "Task") -> dict[str, str]:
        return {
            "state": self.state.current_state.id,
            "parse_id": str(self.parse_id),
        }

    @staticmethod
    def db_deserialize(row: dict[str, str]) -> "Task":
        return Task(
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


@pytest.mark.asyncio()
@pytest.mark.require_db()
async def test_state(engine: AsyncEngine) -> None:
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
        timeout_seconds=60,
    )
    q = TaskQueue(connector=connector)
    task = Task(state=TaskStateMachine(), parse_id=uuid.uuid4())
    task_id = await q.put(task)

    read_msg = await q.pull(state="processing")
    assert read_msg

    assert read_msg.parse_id == str(task.parse_id)

    # Nothing to pull
    assert await q.pull(state="processing") is None

    task = await q.get_task(task_id)
    assert task.state == "processing"

    task = await q.update_task(task_id, state="completed")
    assert task.state == "completed"

    # Nothing to pull
    assert await q.pull(state="processing") is None


@pytest.mark.asyncio()
@pytest.mark.require_db()
async def test_retries(engine: AsyncEngine) -> None:
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
        timeout_seconds=None,
    )
    q = TaskQueue(connector=connector)
    task = Task(state=TaskStateMachine(), parse_id=uuid.uuid4())
    await q.put(task)

    read_msg = await q.pull(state="processing")
    assert read_msg

    assert read_msg.parse_id == str(task.parse_id)

    read_msg_2 = await q.pull(state="processing")
    assert read_msg_2

    read_msg_3 = await q.pull(state="processing")
    assert read_msg_3


@pytest.mark.asyncio()
@pytest.mark.require_db()
async def test_retries_sleep(engine: AsyncEngine) -> None:
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
        timeout_seconds=0.1,
    )
    q = TaskQueue(connector=connector)
    task = Task(state=TaskStateMachine(), parse_id=uuid.uuid4())
    await q.put(task)

    read_msg = await q.pull(state="processing")
    assert read_msg

    assert read_msg.parse_id == str(task.parse_id)

    await asyncio.sleep(0.5)

    read_msg_2 = await q.pull(state="processing")
    assert read_msg_2

    await asyncio.sleep(0.5)

    read_msg_3 = await q.pull(state="processing")
    assert read_msg_3
