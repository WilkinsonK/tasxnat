import asyncio, copy, inspect, multiprocessing as mp, re
import abc, typing
from concurrent.futures import ThreadPoolExecutor, wait as fut_wait
from multiprocessing import pool

_PoolFactory = type[pool.Pool] | typing.Callable[[], pool.Pool]

RE_TASK_CALLER = re.compile(r"^[\w\.\:]+|\[.+\]$")


#NOTE: this is fairly lazy, let alone a 'dumb'
# algorithm, but will work for now.
#TODO: find better parsing solution.
def _parse_task_call(task_call: str) -> tuple[str, tuple[str], dict[str, str]]:
    found = RE_TASK_CALLER.findall(task_call)
    if len(found) == 2:
        caller, rparams = found
    else:
        caller, rparams = found[0], ""

    rparams = rparams.lstrip("[ ").rstrip(" ]") + "\0"

    preparsed, in_quotes = list[str](), False
    seek0, seek1 = 0, 0
    while rparams[seek1] != "\0":
        if rparams[seek1] in ("'", "\""):
            in_quotes = not in_quotes

        seek1 += 1
        if rparams[seek1] == " " and not in_quotes:
            preparsed.append(rparams[seek0:seek1].lstrip())
            seek0 = seek1
            continue
    preparsed.append(rparams[seek0:seek1].lstrip())

    args, kwds = (), {} #type: ignore[annotated]
    for rparam in preparsed:
        # We don't allow implicit empty values.
        # Empty strings are represented as quoted
        # strings.
        if not rparam:
            raise ValueError(f"Illegal implicit empty string.")

        if "=" not in rparam:
            # Remove quotes from param.
            args += (rparam.strip("'\" "),) #type: ignore[assignment]
        else:
            k, v = rparam.split("=", maxsplit=1)

            if not v:
                raise ValueError(f"Illegal implicit empty string.")
            kwds[k] = v.strip("'\" ")

    return caller, args, kwds #type: ignore[return-value]


def _flatten_to_taskmaps(
        *task_calls: str) -> list[tuple[str, typing.Iterable[tuple[tuple, dict]]]]:
    """
    Parses the given task calls grouping callargs
    with their task name. This makes it so all
    similar task calls are grouped together.
    """

    # Collect all task calls in groups to
    # process similar calls together.
    taskable_map = dict[str, tuple]()
    for task_call in task_calls:
        iden, *callargs = _parse_task_call(task_call)
        if iden in taskable_map:
            taskable_map[iden] += (callargs,)
        else:
            taskable_map[iden] = (callargs,)

    # Flatten the taskable map for pool
    # consumption
    return [(iden, calls) for iden, calls in taskable_map.items()]


def _handle_coroutine(coro: typing.Coroutine):
    policy = asyncio.get_event_loop_policy()
    try:
        loop = policy.get_event_loop()
    except RuntimeError:
        loop = policy.new_event_loop()
    return loop.run_until_complete(coro)


def _process_tasks(
        root_task: "Taskable",
        calls: typing.Iterable[tuple[tuple, dict]],
        strict_mode: bool):
    for args, kwds in calls:
        task = copy.deepcopy(root_task)

        task.handle(*args, **kwds)
        if task.is_success:
            continue

        # Bail on first failure if strict mode.
        if strict_mode and task.is_strict:
            if task.failure[1]:
                raise task.failure[1]


def _process_tasks_multi(
        root_task: "Taskable",
        calls: typing.Iterable[tuple[tuple, dict]],
        strict_mode: bool):
    tpool = ThreadPoolExecutor(root_task.thread_count, root_task.identifier)
    # loop = asyncio.get_event_loop_policy().get_event_loop()

    def inner(call: tuple[tuple, dict]):
        task = copy.deepcopy(root_task)
        args, kwds = call
        task.handle(*args, **kwds)

        if task.is_success:
            return

        if strict_mode and task.is_strict:
            _, err = task.failure
            raise err #type: ignore[misc]

    with tpool:
        results = fut_wait([
            tpool.submit(inner, call)
            for call in calls], 30, "FIRST_EXCEPTION")

        for result in results.done:
            result.result()


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
    def task(self, fn: typing.Callable, /) -> "Taskable":
        ...

    @typing.overload
    @abc.abstractmethod
    def task(self, **kwds) -> typing.Callable[[], "Taskable"]:
        ...

    @abc.abstractmethod
    def task(self,
             fn: typing.Callable | None = None, **kwds) -> "Taskable" | typing.Callable[[], "Taskable"]: 
        """
        Creates and registers a `Taskable`
        object.
        """

    @abc.abstractmethod
    def register_task(self, taskable: "Taskable") -> None:
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
    def broker(self) -> TaskBroker:
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
                      broker: TaskBroker,
                      fn: typing.Callable,
                      thread_count: typing.Optional[int],
                      is_strict: typing.Optional[bool],
                      is_async: typing.Optional[bool]) -> typing.Self:
        """
        Create a `Taskable` from a callable
        object.
        """


class SimpleMetaData(typing.TypedDict):
    strict_mode: bool
    task_class: type[Taskable]


class SimpleTaskBroker(TaskBroker):

    _pool_factory: _PoolFactory
    _pool_max_timeout: int = 30

    __metadata__: SimpleMetaData
    __register__: dict[str, Taskable] 

    @property
    def metadata(self):
        return self.__metadata__

    def task(self, #type: ignore[override]
             fn: typing.Optional[typing.Callable] = None,
             *,
             klass: typing.Optional[type[Taskable]] = None,
             thread_count: typing.Optional[int] = None,
             is_strict: typing.Optional[bool] = None,
             is_async: typing.Optional[bool] = None):

        klass = klass or self.metadata["task_class"]

        def wrapper(func) -> Taskable:
            task = klass.from_callable( #type: ignore[union-attr]
                self,
                func,
                thread_count,
                is_strict,
                is_async)
            self.register_task(task)
            return func

        if fn:
            return wrapper(fn)
        return wrapper

    def register_task(self, taskable: "Taskable"):
        self.__register__[taskable.identifier] = taskable

    def process_tasks(self,
                      *task_calls: str,
                      process_count: typing.Optional[int] = None):
        task_call_maps = _flatten_to_taskmaps(*task_calls)

        # Don't even bother with multiproc mode.
        # Run in main thread syncronously.
        if not process_count or process_count == 1:
            for iden, calls in task_call_maps:
                self._process_tasks(iden, calls)
            return

        with mp.Pool(process_count) as p:
            result = p.starmap_async(self._process_tasks, task_call_maps)
            result.get(self._pool_max_timeout)

    def _process_tasks(self,
                       iden: str,
                       calls: typing.Iterable[tuple[tuple, dict]]):
        strict_mode = self.metadata["strict_mode"]
        root_task = self.__register__[iden]

        if root_task.thread_count <= 1:
            _process_tasks(root_task, calls, strict_mode)
        else:
            _process_tasks_multi(root_task, calls, strict_mode)

    @typing.overload
    def __init__(self, /):
        ...

    @typing.overload
    def __init__(self,
                 *,
                 strict_mode: typing.Optional[bool] = None,
                 task_class: typing.Optional[type[Taskable]] = None,
                 pool_factory: typing.Optional[type[pool.Pool]] = None):
        ...

    def __init__(self,
                 *,
                 strict_mode: typing.Optional[bool] = None,
                 task_class: typing.Optional[type[Taskable]] = None,
                 pool_factory: typing.Optional[_PoolFactory] = None):
        self.__metadata__ = (
            {
                "strict_mode": strict_mode or False,
                "task_class": task_class or SimpleTask
            })
        self.__register__ = {}
        self._pool_factory = pool_factory or mp.Pool


class SimpleTask(Taskable):
    _broker: TaskBroker
    _failure_reason: str | None
    _failure_exception: Exception | None
    _is_async: bool
    _is_strict: bool
    _is_success: bool
    _thread_count: int
    _task: typing.Callable

    @property
    def identifier(self):
        return ":".join([self._task.__module__, self._task.__name__])

    @property
    def broker(self):
        return self._broker

    @property
    def failure(self):
        return (self._failure_reason, self._failure_exception)

    @property
    def thread_count(self):
        return self._thread_count

    @property
    def is_async(self):
        return self._is_async

    @property
    def is_strict(self):
        return self._is_strict

    @property
    def is_success(self):
        return self._is_success

    def handle(self, *args, **kwds):
        try:
            result = self._task(*args, **kwds)
            if self.is_async:
                _handle_coroutine(result)
        except Exception as error:
            self._failure_reason = str(error)
            self._failure_exception = error
            return

        self._failure_reason = None
        self._is_success = True

    @classmethod
    def from_callable(cls,
                      broker: TaskBroker,
                      fn: typing.Callable,
                      thread_count: typing.Optional[int] = None,
                      is_strict: typing.Optional[bool] = None,
                      is_async: typing.Optional[bool] = None):
        return cls(broker, fn, thread_count, is_strict, is_async)

    def __init__(self,
                 broker: TaskBroker,
                 fn: typing.Callable,
                 thread_count: typing.Optional[int] = None,
                 is_strict: typing.Optional[bool] = None,
                 is_async: typing.Optional[bool] = None):
        self._broker = broker
        self._failure_reason = "Task was never handled."
        self._failure_exception = None
        self._task = fn

        self._thread_count = thread_count or 1

        # Flag parsing goes here.
        self._is_async = (
            is_async if is_async is not None
            else inspect.iscoroutinefunction(fn))
        self._is_strict = is_strict or False
        self._is_success = False


if __name__ == "__main__":
    broker = SimpleTaskBroker(strict_mode=True)

    @broker.task(is_strict=True)
    def say_hello(name: str = "Duey", age: int = 0):
        print(f"Hello {name}! Your age is {age} years")

    @broker.task(is_strict=True, is_async=False)
    async def asay_hello(name: str):
        print(f"Hello {name}")

    task_calls = (
        "__main__:asay_hello[Keenan]",
        "__main__:asay_hello[Ryan]",
        "__main__:asay_hello[Helen]",
        "__main__:say_hello[ '' 14]",
        "__main__:say_hello['Klayton' 17 ]",
        "__main__:say_hello[Keenan 27]",
        "__main__:say_hello[\"Lucy\" age='32']",
        "__main__:say_hello['Huey Luis']")

    broker.process_tasks(*task_calls, process_count=1)
