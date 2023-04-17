import inspect, multiprocessing as mp
import typing
from multiprocessing import pool

from tasxnat.protocols import Taskable, TaskBroker, _PoolFactory
from tasxnat.utilities import *


__all__ = (
    (
        "SimpleTaskBroker",
        "SimpleTaskable"
    ))

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
                "task_class": task_class or SimpleTaskable
            })
        self.__register__ = {}
        self._pool_factory = pool_factory or mp.Pool


class SimpleTaskable(Taskable):
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
