from __future__ import annotations

from typing import Generic, Optional, Protocol, TypeVar

IdType = TypeVar("IdType")
TaskType = TypeVar("TaskType")
StateType = TypeVar("StateType")


class Connector(Protocol):
    async def put(self, task: TaskType) -> IdType: ...

    async def pull(
        self,
        patch_values: dict[str, str],
        locked_by: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
    ) -> Optional[TaskType]: ...

    async def get_task(self, task_id: IdType) -> TaskType: ...

    async def patch_task(
        self,
        task_id: IdType,
        values: dict[str, str],
    ) -> TaskType: ...


class Queue(Generic[IdType, TaskType]):
    def __init__(
        self,
        connector: Connector,
    ) -> None:
        self.connector = connector

    async def put(self, task: TaskType) -> IdType:
        return await self.connector.put(task=task)

    async def pull(
        self,
        patch_values: dict[str, str],
        locked_by: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
    ) -> Optional[TaskType]:
        return await self.connector.pull(
            patch_values=patch_values,
            locked_by=locked_by,
            timeout_seconds=timeout_seconds,
        )

    async def get_task(self, task_id: IdType) -> TaskType:
        return await self.connector.get_task(task_id)

    async def patch_task(
        self,
        task_id: IdType,
        values: dict[str, str],
    ) -> TaskType:
        return await self.connector.patch_task(task_id, values=values)


class TaskNotFoundError(ValueError, Generic[IdType]):
    def __init__(self, task_id: IdType) -> None:
        pass
