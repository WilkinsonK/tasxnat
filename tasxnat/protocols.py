import abc, typing
from multiprocessing import pool

__all__ = (
    (
        "_PoolFactory",
        "TaskBroker",
        "Taskable"
    ))

_PoolFactory = type[pool.Pool] | typing.Callable[[], pool.Pool]


@typing.runtime_checkable
class Taskable(typing.Protocol):
    """
    Handles some task defined by this class.
    """

    @property
    @abc.abstractmethod
    def identifier(self) -> str:
        """Identifier of this `Taskable`."""

    @property
    @abc.abstractmethod
    def broker(self) -> "TaskBroker":
        """Parent `TaskBroker`."""

    @property
    @abc.abstractmethod
    def failure(self) -> tuple[str | None, Exception | None]:
        """Failure details."""

    @property
    @abc.abstractmethod
    def thread_count(self) -> int:
        """
        Number of threads this task is allowed to
        run in at one time.
        """

    @property
    @abc.abstractmethod
    def is_async(self) -> bool:
        """
        Whether this task is an asyncronous
        callable.
        """

    @property
    @abc.abstractmethod
    def is_strict(self) -> bool:
        """
        Whether this task should cause subsequent
        tasks to fail/not execute.
        """

    @property
    @abc.abstractmethod
    def is_success(self) -> bool:
        """
        Whether this task completed successfully.
        """

    @abc.abstractmethod
    def handle(self, *args, **kwds) -> None:
        """
        Executes this task with the arguments
        passed.
        """

    @classmethod
    @abc.abstractmethod
    def from_callable(cls,
                      broker: "TaskBroker",
                      fn: typing.Callable,
                      thread_count: typing.Optional[int],
                      is_strict: typing.Optional[bool],
                      is_async: typing.Optional[bool]) -> typing.Self:
        """
        Create a `Taskable` from a callable
        object.
        """


@typing.runtime_checkable
class TaskBroker(typing.Protocol):
    """
    Manages `Taskable` objects. This includes
    instantiation, execution and evaluation of
    execution results.
    """

    @property
    @abc.abstractmethod
    def metadata(self) -> typing.Mapping[str, str]:
        """Task metadata."""

    @typing.overload
    @abc.abstractmethod
    def task(self, fn: typing.Callable, /) -> Taskable:
        ...

    @typing.overload
    @abc.abstractmethod
    def task(self, **kwds) -> typing.Callable[[], Taskable]:
        ...

    @abc.abstractmethod
    def task(self,
             fn: typing.Callable | None = None, **kwds) -> Taskable | typing.Callable[[], Taskable]: 
        """
        Creates and registers a `Taskable`
        object.
        """

    @abc.abstractmethod
    def register_task(self, taskable: Taskable) -> None:
        """
        Register a `Taskable` object to this.
        task manager.
        """

    @typing.overload
    @abc.abstractmethod
    def process_tasks(self, *task_callers: str) -> None:
        ...

    @typing.overload
    @abc.abstractmethod
    def process_tasks(self,
                      *task_callers: str,
                      process_count: typing.Optional[int]) -> None:
        ...

    @abc.abstractmethod
    def process_tasks(self,
                      *task_callers: str,
                      process_count: typing.Optional[int] = None) -> None:
        """
        Executes given tasks from their
        identifiers.

        :task_callers: series of strings in the
        format of `<import.path>:<task_name>`.
        """
