from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generator, Optional

import pytest
from sqlalchemy import Column, DateTime, MetaData, Table, func, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from snailqueue import Queue, SqlAlchemyConnector

if TYPE_CHECKING:
    import uuid

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


task_table = Table(
    "task",
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
        table=task_table,
        id_column=task_table.c.id,
        priority_column=task_table.c.time_created,
        locked_by_name_column=task_table.c.locked_by_name,
        locked_by_time_column=task_table.c.locked_by_time,
        serializer=Message.db_serialize,
        deserializer=Message.db_deserialize,
    )
    q = MessageQueue(connector=connector, name="xxx")
    msg = Message()
    write_message_id = await q.put(msg)

    read_msg = await q.pull()
    assert read_msg

    assert read_msg.message_id == write_message_id
