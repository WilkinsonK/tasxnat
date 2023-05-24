"""
Simple tasking interface. Objects in this package
are used to broker calls to registered tasks.
"""

__version__ = (0, 1, 0)


# What should the new interface look like?
import typing

Ps = typing.ParamSpec("Ps")
Rt = typing.TypeVar("Rt")
Tasked   = typing.Callable[typing.Concatenate[Ps], Rt]
Taskable = Tasked[Ps, Rt] | type[Tasked[Ps, Rt]]
TaskId   = typing.Hashable


class TaskConfig:
    _broker: "TaskBroker" | None = None
    _caller: Tasked
    _is_async: bool = False
    _is_strict: bool = False

    @property
    def broker(self):
        return self._broker

    @property
    def caller(self):
        return self._caller

    @property
    def is_async(self):
        return self._is_async

    @property
    def is_strict(self):
        return self._is_strict

    def __init__(self, taskable: Taskable, **kwds) -> None:
        if isinstance(taskable, type):
            self._caller = taskable(**kwds)
        else:
            self._caller = taskable


class TaskResult(typing.Generic[Rt]):
    result: Rt | None
    _failure: Exception | None

    @property
    def failure(self):
        return self._failure

    @property
    def failure_reason(self):
        return str(self._failure)

    @property
    def is_success(self) -> bool:
        return self._failure is None

    def __init__(self):
        self.result = None
        self._failure = None


class Task(typing.Generic[Ps, Rt]):
    __config__: TaskConfig

    class Config(TaskConfig):
        ...

    @property
    def config(self):
        return self.__config__

    @property
    def parent(self):
        return self.config.broker

    def handle(self, *args, **kwds) -> TaskResult[Rt]:
        if self.parent:
            args = (self.parent, *args)

        result = TaskResult[Rt]()
        try:
            result.result = self.config.caller(*args, **kwds)
        except Exception as error:
            if self.config.is_strict:
                raise
            result._failure = error

        return result

    def __init__(self, taskable: Taskable, **kwds):
        self.__config__ = self.Config(taskable)


class TaskBroker:
    __register__: dict[TaskId, Taskable]

    def register(self, task: Task):
        ...

    def schedule(self, task: Task, *args, **kwds):
        ...

    def taskable(self, taskable: Taskable, *_, **kwds) -> Task:
        ...

    def broker(self, task: Task) -> Task:
        task.config._broker = self
        return task


broker = TaskBroker()


@broker.taskable
class SomeTaskableObject:

    def __call__(self, *args, **kwds):
        ...


@broker.taskable
def some_taskable_func(*args, **kwds):
    ...
