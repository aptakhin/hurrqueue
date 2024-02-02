from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generator, Optional

import pytest
from sqlalchemy import Column, DateTime, MetaData, String, Table, func, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from statemachine import State, StateMachine
from statemachine.model import Model

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


class MessageQueue(Queue[Message]):
    pass


@pytest.fixture()
def database_url() -> str:
    return "postgresql+asyncpg://postgres:postgres@localhost:5432/postgres"


@pytest.fixture()
async def engine(database_url: str) -> Generator[AsyncEngine, None, None]:
    engine = create_async_engine(
        database_url,
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
    Column("locked_by_name", UUID),
    Column("locked_by_time", DateTime(timezone=True)),
    Column("time_created", DateTime(timezone=True), server_default=func.now()),
)


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
    q = MessageQueue(connector=connector, name="xxx")
    msg = Message()
    write_message_id = await q.put(msg)

    read_msg = await q.pull()
    assert read_msg

    assert read_msg.message_id == write_message_id


# https://github.com/fgmacedo/python-statemachine
class TaskStateMachine(StateMachine):
    enqueued = State(initial=True)
    processing = State()
    # timeout = State()
    completed = State(final=True)

    switch_to_processing = enqueued.to(processing)
    switch_to_completed = processing.to(completed)

    def __init__(self) -> None:
        self.timeouted = 0


@dataclass
class Task():
    state: TaskStateMachine
    parse_id: "uuid.UUID"

    def db_serialize(self: "Task") -> dict[str, str]:
        return {
            "state": "q",
            "parse_id": str(self.parse_id),
        }

    @staticmethod
    def db_deserialize(row: dict[str, str]) -> "Message":
        return Task(
            state=row["state"],
            parse_id=row["parse_id"],
        )


class TaskQueue(Queue[Message]):
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
    Column("locked_by_name", UUID),
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
    )
    q = TaskQueue(connector=connector, name="xxx")
    task = Task(state=TaskStateMachine(), parse_id=uuid.uuid4())
    write_message_id = await q.put(task)

    read_msg = await q.pull()
    assert read_msg

    assert read_msg.parse_id == str(task.parse_id)

    # await pull_task()
    # nothing to pull here with other name

    # assert state == "processing"

    # finish_task()

    # assert state == "timeout"