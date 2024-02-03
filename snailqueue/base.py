from __future__ import annotations

from typing import Generic, Optional, Protocol, TypeVar

IdType = TypeVar("IdType")
TaskType = TypeVar("TaskType")
StateType = TypeVar("StateType")


class Connector(Protocol):
    async def put(self, task: TaskType) -> IdType: ...

    async def pull(
        self,
        state: Optional[StateType] = None,
        locked_by: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
    ) -> Optional[TaskType]: ...

    async def get_task(self, task_id: IdType) -> TaskType: ...

    async def update_task(
        self,
        task_id: IdType,
        state: Optional[str] = None,
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
        state: Optional[StateType] = None,
        locked_by: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
    ) -> Optional[TaskType]:
        return await self.connector.pull(
            state=state,
            locked_by=locked_by,
            timeout_seconds=timeout_seconds,
        )

    async def get_task(self, task_id: IdType) -> TaskType:
        return await self.connector.get_task(task_id)

    async def update_task(
        self,
        task_id: IdType,
        state: Optional[str] = None,
    ) -> TaskType:
        return await self.connector.update_task(task_id, state=state)


class TaskNotFoundError(ValueError, Generic[IdType]):
    def __init__(self, task_id: IdType) -> None:
        pass
