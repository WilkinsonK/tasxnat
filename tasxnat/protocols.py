import abc, typing
from multiprocessing import pool

__all__ = (
    (
        "_PoolFactory",
        "TaskedCallable",
        "TaskBroker",
        "Taskable"
    ))

_Ps = typing.ParamSpec("_Ps")
_RT = typing.TypeVar("_RT")
_RT_co = typing.TypeVar("_RT_co", covariant=True)
_PoolFactory = type[pool.Pool] | typing.Callable[[], pool.Pool]
_TCStackCallable = typing.Callable[["TaskedCallable"], None]
_TaskableCallable = typing.Callable[typing.Concatenate["Taskable", _Ps], _RT]


class _TCStack(typing.Sequence[_TCStackCallable]):
    """
    Sequence of callable objects run in First In
    First Called (FIFC).
    """

    @abc.abstractmethod
    def append(self, item: _TCStackCallable) -> None:
        """
        Push the given item to the end of this
        call stack.
        """


@typing.runtime_checkable
class TaskedCallable(typing.Protocol[_Ps, _RT_co]):
    """
    Callable object that is registered to some
    `TaskBroker` object.
    """

    @property
    @abc.abstractmethod
    def args(self) -> typing.ParamSpecArgs:
        """VarArgs passed into this callable."""

    @args.setter
    @abc.abstractmethod
    def args(self, *args: typing.ParamSpecArgs):
        """
        Set the VarArgs passed into this
        callable.
        """

    @property
    @abc.abstractmethod
    def kwds(self) -> typing.ParamSpecKwargs:
        """
        Keyword VarArgs passed into this
        callable.
        """

    @property
    def is_async(self) -> bool:
        """
        Whether this task is an asyncronous
        callable.
        """

    @property
    @abc.abstractmethod
    def taskable(self) -> "Taskable":
        """The parent taskable object."""

    @abc.abstractmethod
    def push_before(self, fn: _TCStackCallable) -> None:
        """
        Push some callable object onto the
        `before task` call stack.
        """

    @abc.abstractmethod
    def push_after(self, fn: _TCStackCallable) -> None:
        """
        Push some callable object onto the
        `after task` call stack.

        Note: *after task* callables must return
        the expected return value of this tasks
        return value.
        """

    @abc.abstractmethod
    def __init__(self, parent: "Taskable", fn: _TaskableCallable, **kwds):
        """
        Construct a `TaskedCallable` object.
        """

    @abc.abstractmethod
    def __call__(self, *args: _Ps.args, **kwds: _Ps.kwargs) -> _RT_co:
        """
        Some function or callable object which
        can be called via a `Taskable` object.
        """

    @abc.abstractmethod
    def __before__(self) -> None:
        """
        Runs the assigned stack of procedures
        *before* calling this task.
        """

    @abc.abstractmethod
    def __after__(self) -> None:
        """
        Runs the assigned stack of procedures
        *after* calling this task.
        """


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
    def task(self, fn: typing.Callable, /) -> TaskedCallable:
        ...

    @typing.overload
    @abc.abstractmethod
    def task(self,
             *,
             klass: typing.Optional[type[Taskable]],
             thread_count: typing.Optional[int],
             is_strict: typing.Optional[bool],
             is_async: typing.Optional[bool]) -> typing.Callable[[], TaskedCallable]:
        ...

    @abc.abstractmethod
    def task(self,
             fn: typing.Callable | None = None,
             *,
             klass: typing.Optional[type[Taskable]] = None,
             thread_count: typing.Optional[int] = None,
             is_strict: typing.Optional[bool] = None,
             is_async: typing.Optional[bool] = None) -> TaskedCallable | typing.Callable[[], TaskedCallable]: 
        """
        Creates and registers a `Taskable`
        object.
        """

    @typing.overload
    @abc.abstractmethod
    def before(
        self,
        fn: _TCStackCallable, /) -> typing.Callable[[], TaskedCallable]:
        ...

    @typing.overload
    @abc.abstractmethod
    def before(
        self,
        fn1: TaskedCallable,
        fn2: _TCStackCallable, /) -> TaskedCallable:
        ...

    @typing.overload
    @abc.abstractmethod
    def before(
        self,
        fn1: TaskedCallable | _TCStackCallable,
        fn2: TaskedCallable | _TCStackCallable | None, /) -> TaskedCallable | typing.Callable[[], TaskedCallable]:
        ...

    @abc.abstractmethod
    def before(
        self,
        fn1: TaskedCallable | _TCStackCallable,
        fn2: TaskedCallable | _TCStackCallable | None = None) -> TaskedCallable | typing.Callable[[], TaskedCallable]:
        """
        Push the target callable on to the
        *before* stack of the given
        `TaskedCallable`.
        """

    @typing.overload
    @abc.abstractmethod
    def after(
        self,
        fn: _TCStackCallable, /) -> typing.Callable[[], TaskedCallable]:
        ...

    @typing.overload
    @abc.abstractmethod
    def after(
        self,
        fn1: TaskedCallable,
        fn2: _TCStackCallable, /) -> TaskedCallable:
        ...

    @typing.overload
    @abc.abstractmethod
    def after(
        self,
        fn1: TaskedCallable | _TCStackCallable,
        fn2: TaskedCallable | _TCStackCallable | None, /) -> TaskedCallable | typing.Callable[[], TaskedCallable]:
        ...

    @abc.abstractmethod
    def after(
        self,
        fn1: TaskedCallable | _TCStackCallable,
        fn2: TaskedCallable | _TCStackCallable | None = None) -> TaskedCallable | typing.Callable[[], TaskedCallable]:
        """
        Push the target callable on to the
        *after* stack of the given
        `TaskedCallable`.
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
