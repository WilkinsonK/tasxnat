"""
Simple tasking interface. Objects in this package
are used to broker calls to registered tasks.
"""

__version__ = (0, 1, 0)


# What should the new interface look like?
import asyncio, inspect
import typing

Ps = typing.ParamSpec("Ps")
Rt = typing.TypeVar("Rt")
Tasked   = typing.Callable[typing.Concatenate[Ps], Rt]
Taskable = Tasked[Ps, Rt] | type[Tasked[Ps, Rt]]
TaskId   = typing.Hashable


def synch_tasked_handler(tasked: Tasked, args: tuple, kwds: dict):
    return tasked(*args, **kwds)


def async_tasked_handler(tasked: Tasked, args: tuple, kwds: dict):
    with asyncio.Runner() as runner:
        return runner.run(tasked(*args, **kwds))


class TaskConfig:
    _broker: typing.Optional["TaskBroker"]
    _caller: Tasked
    _is_async: bool
    _is_strict: bool

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
        if isinstance(taskable, type) and hasattr(taskable, "__call__"):
            self._caller = taskable(**kwds)
            func = taskable.__call__
        else:
            self._caller = taskable
            func = taskable

        self._broker = None
        self._is_async  = inspect.iscoroutinefunction(func)


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
    def name(self):
        if hasattr(self.config.caller, "__name__"):
            name = self.config.caller.__name__
        else:
            name = self.config.caller.__class__.__name__

        return ":".join([self.config.caller.__module__, name])

    @property
    def parent(self):
        return self.config.broker

    def handle(self, *args, **kwds) -> TaskResult[Rt]:
        if self.parent:
            args = (self.parent, *args)

        # TODO: finish the handle code for both
        # handler functions
        if self.config.is_async:
            handler = async_tasked_handler
        else:
            handler = synch_tasked_handler

        result = TaskResult[Rt]()
        try:
            result = handler(self.config.caller, args, kwds)
        except Exception as error:
            if self.config.is_strict:
                raise
            result._failure = error

        return result

    def __init__(self, taskable: Taskable, **kwds):
        self.__config__ = self.Config(taskable, **kwds)


class TaskBroker:
    __register__: dict[TaskId, Task]

    def register(self, task: Task) -> None:
        self.__register__[task.name] = task

    def schedule(self, task: Task, *args, **kwds):
        ...

    def broker(self, broker: typing.Optional[typing.Self] = None) -> typing.Callable[[Task], Task]:

        def set_broker(task: Task):
            task.config._broker = broker or self
            return task

        return set_broker

    def taskable(self, taskable: Taskable | None = None, **kwds) -> Task | typing.Callable[[Taskable], Task]:

        def create_task(taskable: Taskable):

            def inner(**kwds):
                task = Task(taskable, **kwds) #type: ignore[var-annotated]
                self.register(task)
                return task

            return inner(**kwds)

        if taskable:
            return create_task(taskable)
        else:
            return create_task

    def strict(self, strict: typing.Optional[bool] = None) -> typing.Callable[[Task], Task]:

        def set_strict(task: Task):
            task.config._is_strict = strict or False
            return task
        
        return set_strict

    def __init__(self):
        self.__register__ = {}


broker = TaskBroker()


@broker.taskable
class SomeTaskableObject:

    async def __call__(self, *args):
        ...


@broker.taskable
async def some_taskable_func(*args):
    ...
