import inspect, time, multiprocessing as mp
import typing
from multiprocessing import pool

from tasxnat.protocols import\
(
    Taskable,
    TaskBroker,
    TaskedCallable,
    _PoolFactory,
    _TaskableCallable,
    _TCStackCallable,
    _TCStack
)
from tasxnat.utilities import *


__all__ = (
    (
        "SimpleTaskBroker",
        "SimpleTaskable",
        "SimpleTaskedCallable",
        "AsyncTaskedCallable"
    ))


def _simple_identifier(task: typing.Callable):
    return ":".join([task.__module__, task.__name__])


class SimpleMetaData(typing.TypedDict):
    strict_mode: bool
    task_class: type[Taskable]



class SimpleTaskedCallable(TaskedCallable):
    _is_async: bool

    __taskable__: "Taskable"
    __task__: _TaskableCallable
    __before_tasks__: _TCStack
    __after_tasks__: _TCStack

    @property
    def args(self):
        return self.__called_args__

    @args.setter
    def args(self, args):
        self.__called_args__ = args

    @property
    def kwds(self):
        return self.__called_kwds__

    @property
    def is_async(self):
        return self._is_async

    @property
    def taskable(self):
        return self.__taskable__

    def push_before(self, fn):
        self.__before_tasks__.append(fn)

    def push_after(self, fn):
        self.__after_tasks__.append(fn)

    def __init__(self, parent, fn, *, is_async: bool | None = None):
        self.__taskable__ = parent
        self.__task__ = fn
        self.__before_tasks__ = [] #type: ignore[assignment]
        self.__after_tasks__ = [] #type: ignore[assignment]

        self._is_async = is_async or False

        setattr(self, "__name__", fn.__name__)
        setattr(self, "__module__", fn.__module__)

    def __call__(self, *args, **kwds):
        self.__called_args__ = args
        self.__called_kwds__ = kwds

        self.__before__()
        rt = self.__task__(self.taskable, *self.args, **self.kwds)
        self.__after__()

        return rt

    def __before__(self):
        for fn in reversed(self.__before_tasks__):
            fn(self)

    def __after__(self) -> None:
        for fn in reversed(self.__after_tasks__):
            fn(self)


class AsyncTaskedCallable(SimpleTaskedCallable):

    async def __call__(self, *args, **kwds):
        self.__called_args__ = args
        self.__called_kwds__ = kwds

        await self.__before__()
        rt = await self.__task__(self.__taskable__, *self.args, **self.kwds)
        await self.__after__()

        return rt

    async def __before__(self):
        for fn in reversed(self.__before_tasks__):
            await self._handle_procedure(fn)

    async def __after__(self):
        for fn in reversed(self.__after_tasks__):
            await self._handle_procedure(fn)

    async def _handle_procedure(self, fn: _TCStackCallable):
        if inspect.iscoroutinefunction(fn):
            await fn(self)
        else:
            fn(self)


class SimpleTaskable(Taskable):
    callable_class: typing.ClassVar[type[TaskedCallable] | None] = None

    _broker: TaskBroker
    _failure_reason: str | None
    _failure_exception: Exception | None
    _is_strict: bool
    _is_success: bool
    _thread_count: int
    _task: TaskedCallable

    @property
    def identifier(self):
        return _simple_identifier(self._task)

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
        return self._task.is_async

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
                      broker,
                      fn,
                      thread_count=None,
                      is_strict=None,
                      is_async=None):
        return cls(broker, fn, thread_count, is_strict, is_async)

    def set_thread_pool(self, pool, queue):
        if self.thread_count <= 1:
            raise RuntimeError(f"Threading was not enable for this task.")

        self._thread_pool = pool
        self._thread_queue = queue

    def request_new_thread(
            self,
            fn,
            callargs,
            *,
            timeout: int | float | None = None):

        if self.thread_count <= 1:
            raise RuntimeError(f"Threading was not enable for this task.")

        tqueue = self._thread_queue
        request_t = time.monotonic()
        while (len(tqueue) + 1) == tqueue.maxlen:
            time.sleep(0.1)
            curr_t = time.monotonic()
            if timeout and (curr_t - request_t) > timeout:
                raise TimeoutError("Thread request took too long.")

        tqueue.append((fn, callargs))

    def __init__(self,
                 broker: TaskBroker,
                 fn: typing.Callable,
                 thread_count: typing.Optional[int] = None,
                 is_strict: typing.Optional[bool] = None,
                 is_async: typing.Optional[bool] = None):
        self._broker = broker
        self._failure_reason = "Task was never handled."
        self._failure_exception = None

        is_async =\
        is_async if is_async is not None else inspect.iscoroutinefunction(fn)

        if not self.callable_class:
            if is_async:
                callable_class = AsyncTaskedCallable
            else:
                callable_class = SimpleTaskedCallable #type: ignore[assignment]
            self._task = callable_class(self, fn, is_async=is_async)
        else:
            self._task = self.callable_class(self, fn)

        self._thread_count = thread_count or 1

        # Flag parsing goes here.
        self._is_strict = is_strict or False
        self._is_success = False


class SimpleTaskBroker(TaskBroker):

    _pool_factory: _PoolFactory
    _pool_max_timeout: typing.ClassVar[float | int] = 30

    __metadata__: SimpleMetaData
    __register__: dict[str, Taskable] 

    @property
    def metadata(self):
        return self.__metadata__

    def task(self,
             fn=None,
             *,
             klass=None,
             thread_count=None,
             is_strict=None,
             is_async=None):

        klass = klass or self.metadata["task_class"]

        def wrapper(func) -> TaskedCallable:
            task = klass.from_callable(
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

    def before(self, fn1, fn2=None):
        task_getter = lambda fn:  self.__register__[_simple_identifier(fn)]

        if fn2:
            task = task_getter(fn1)
            task._task.push_before(fn2)
            return fn1

        def inner(fn):
            task = task_getter(fn)
            task._task.push_before(fn1)
            return fn

        return inner

    def after(self, fn1, fn2=None):
        task_getter = lambda fn:  self.__register__[_simple_identifier(fn)]

        if fn2:
            task = task_getter(fn1)
            task._task.push_after(fn2)
            return fn1

        def inner(fn):
            task = task_getter(fn)
            task._task.push_after(fn1)
            return fn

        return inner

    def register_task(self, taskable):
        self.__register__[taskable.identifier] = taskable

    def process_tasks(self, *task_callers, process_count=None):
        task_call_maps = _flatten_to_taskmaps(*task_callers)

        # Don't even bother with multiproc mode.
        # Run in main thread syncronously.
        if not process_count or process_count == 1:
            for iden, calls in task_call_maps:
                self._process_tasks(iden, calls)
            return

        with mp.Pool(process_count) as p:
            result = p.starmap_async(self._process_tasks, task_call_maps)
            result.get(self._pool_max_timeout)

    def _process_tasks(
            self,
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
