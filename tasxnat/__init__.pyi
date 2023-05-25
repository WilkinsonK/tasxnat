import asyncio, typing

_Ps = typing.ParamSpec("_Ps")
_Rt = typing.TypeVar("_Rt")
_Rt_co = typing.TypeVar("_Rt_co", covariant=True)

Tasked      = typing.Callable[_Ps, _Rt]
Taskable    = typing.Callable[_Ps, _Rt] | type[typing.Callable[_Ps, _Rt]]
TaskId      = typing.Hashable
TaskWrapper = typing.Callable[[Taskable], Taskable] | typing.Callable[[Task], Task]

def get_broker(id: typing.Hashable = ..., **kwds) -> TaskBroker:
    """
    Retrieve a `TaskBroker` from the registry.
    if no id is given return the root_broker. If
    the id is not registered, create a new
    instance.
    """

class TaskBroker:
    """
    Manages creation and scheduling of `Task`s.
    """
    @property
    def name(self):
        """Name of this `TaskBroker`."""
    @property
    def runner(self) -> asyncio.Runner:
        """
        `asyncio.Runner` owned by this
        `TaskBroker`.
        """
    @typing.overload
    def broker(self, task: Taskable) -> Taskable: ...
    @typing.overload
    def broker(self, task: Task) -> Task: ...
    @typing.overload
    def broker(self) -> TaskWrapper:
        """
        Marks the wrapped task to pass this
        `TaskBroker` in the call args whenever it
        is scheduled.
        """
    def register(self, task: Task) -> None:
        """
        Register a `Task` object to this
        `TaskBroker`.
        """
    @typing.overload
    def schedule(self, task: Task[_Ps, _Rt]) -> TaskResult[_Rt]: ...
    @typing.overload
    def schedule(self, task: Taskable[_Ps, _Rt]) -> TaskResult[_Rt]:
        """
        Submit the `Task` to be run by a worker.

        Returns a future of `TaskResult` object.
        """
    def shutdown(self):
        """
        Cancel any pending tasks and perform
        clean-up as necessary.

        If any tasks are still being processed,
        wait for them to complete.
        """
    @typing.overload
    def strict(self, task: Taskable, /) -> Taskable: ...
    @typing.overload
    def strict(self, task: Task, /) -> Task: ...
    @typing.overload
    def strict(self, **kwds) -> TaskWrapper:
        """
        Sets whether the wrapped `Task` is strict
        and raises any errors that occur.
        """
    @typing.overload
    def taskable(self, taskable: Taskable, /) -> Taskable: ...
    @typing.overload
    def taskable(self, **kwds) -> typing.Callable[[Taskable], Taskable]:
        """
        Registers a callable object as a `Task`
        to this `TaskBroker`.
        """
    def __repr__(self) -> str: ...
    def __init__(self, **kwds) -> None: ...
    def __del__(self) -> None: ...

class Task(typing.Generic[_Ps, _Rt_co]):
    """
    Representative object of some `Taskable`
    which manages and handles it accordingly.
    """
    class Config(TaskConfig):
        """
        Configuration of this `Task`.
        """
    @property
    def config(self) -> Config:
        """Configuration of this `Task`."""
    @property
    def name(self) -> typing.LiteralString:
        """Name of this `Task`."""
    @property
    def  parent(self):
        """Parent `TaskBroker` instance."""
    def handle(self, *args, **kwds) -> TaskResult[_Rt_co]:
        """
        Execute this `Task` and return it's
        result information.
        """
    def __init__(self, broker: TaskBroker, taskable: Taskable, **kwds) -> None: ...
    def __repr__(self) -> str: ...

class TaskConfig(typing.Protocol):
    """Configuration of some `Task`."""
    @property
    def broker(self) -> TaskBroker:
        """Parent `TaskBroker`."""
    @property
    def caller(self) -> Tasked:
        """Callable owned by the `Task`."""
    @property
    def include_broker(self) -> bool:
        """
        Whether the parent `TaskBroker` is passed
        into the task call.
        """
    @property
    def is_async(self) -> bool:
        """Whether this `Task` is asyncronous."""
    @property
    def is_strict(self) -> bool:
        """
        Whether this `Task` raises an error on
        failure.
        """
    def __init__(self, broker: TaskBroker, taskable: Taskable, **kwds) -> None: ...

class TaskResult(typing.Generic[_Rt_co]):
    """
    Result from an attempted handle of some
    `Task`.
    """
    @property
    def failure(self) -> Exception | None:
        """
        Error that occured during handling of the
        `Task`.
        """
    @property
    def failure_reason(self) -> str | None:
        """Reason why the handle failed."""
    @property
    def is_success(self) -> bool:
        """Whether the handle was successful."""
    @property
    def result(self) -> _Rt_co | None:
        """Resulting outcome from handle."""
    def __init__(self) -> None: ...

class TaskTimeWarning(RuntimeWarning):
    """
    Warns for potential issues where a `Task`
    never complets.
    """
