"""
Simple tasking interface. Objects in this package
are used to broker calls to registered tasks.
"""

__version__ = (0, 1, 0)


# What should the new interface look like?
import asyncio, collections, inspect, multiprocessing.pool as mpp, time
import typing, warnings

Ps = typing.ParamSpec("Ps")
Rt = typing.TypeVar("Rt")
Tasked       = typing.Callable[typing.Concatenate[Ps], Rt]
Taskable     = Tasked[Ps, Rt] | type[Tasked[Ps, Rt]]
TaskId       = typing.Hashable


def register_task(
        broker: "TaskBroker",
        taskable: Taskable | None,
        task_cls: type["Task"], **kwds):

    def inner(wrapped):

        def wrapper(kwds):
            task = task_cls(broker, wrapped, **kwds) #type: ignore[var-annotated]
            broker.register(task)
            return task

        return wrapper(kwds)

    if taskable:
        return inner(taskable)
    else:
        return inner


def use_task_broker(task: typing.Optional["Task"] | None):

    def inner(wrapped: Task):
        wrapped.config._include_broker = True
        return wrapped

    if task:
        return inner(task)
    else:
        return inner


def set_task_strictness(task: typing.Optional["Task"], strict: bool):

    def inner(wrapped: Task):
        wrapped.config._is_async = strict
        return wrapped

    if task:
        return inner(task)
    else:
        return inner


class TaskConfig:
    _broker: typing.Optional["TaskBroker"]
    _caller: Tasked
    _include_broker: bool
    _is_async: bool
    _is_strict: bool

    @property
    def broker(self):
        return self._broker

    @property
    def caller(self):
        return self._caller

    @property
    def include_broker(self):
        return self._include_broker

    @property
    def is_async(self):
        return self._is_async

    @property
    def is_strict(self):
        return self._is_strict

    def __init__(self, broker: "TaskBroker", taskable: Taskable, **kwds) -> None:
        if isinstance(taskable, type) and hasattr(taskable, "__call__"):
            self._caller = taskable(**kwds)
            func = taskable.__call__
        else:
            self._caller = taskable
            func = taskable

        self._broker = broker
        self._include_broker = False
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
        """Handle the wrapped callable."""

        if self.config.include_broker:
            args = (self.parent, *args)

        result = TaskResult[Rt]()
        try:
            if self.config.is_async:
                rt = self._handle_async(*args, **kwds)
            else:
                rt = self._handle_synch(*args, **kwds)

            result.result = rt
        except Exception as error:
            if self.config.is_strict:
                raise
            result._failure = error
            warnings.warn(
                f"{self.name} failed with message: {error!r}",
                TaskTimeWarning)

        return result

    def _handle_async(self, *args, **kwds):
        return self.parent.runner.run(self.config.caller(*args, **kwds))

    def _handle_synch(self, *args, **kwds):
        return self.config.caller(*args, **kwds)

    def __init__(self, broker: "TaskBroker", taskable: Taskable, **kwds):
        self.__config__ = self.Config(broker, taskable, **kwds)

    def __repr__(self):
        idn = hex(id(self))
        return f"<{self.__class__.__qualname__}({self.name}) at {idn}>"


class TaskBroker:
    _registry: dict[TaskId, Task]
    _runner:   asyncio.Runner
    _task_pool: mpp.ThreadPool
    _task_queue: collections.deque[mpp.AsyncResult]

    @property
    def queue(self):
        return self._task_queue

    @property
    def registry(self):
        return self._registry.copy()

    @property
    def runner(self):
        return self._runner

    def broker(self, task: Task | None = None) -> Task:
        """
        Marks the wrapped task to pass this
        `TaskBroker` in the call args whenever it
        is scheduled.
        """

        return use_task_broker(task)

    def register(self, task: Task) -> None:
        """
        Adds a `Task` object to this `TaskBroker`
        registry. Registered tasks can be
        scheduled by this broker.
        """

        self._registry[task.name] = task

    def schedule(self, task: Task, *args, **kwds):
        """
        Send the `Task` to be run by a worker.
        """

        def handle_task():
            return task.handle(*args, **kwds)

        self._task_queue.append(self._task_pool.apply_async(handle_task))

    def shutdown(self):
        self._runner.close()
        self._task_pool.close()
        self._task_queue.clear()

    def strict(
            self,
            task: Task | None = None,
            *,
            strict: typing.Optional[bool] = None) -> Task:
        """
        Sets whether the wrapped `Task` is strict
        and raises any errors that occur.
        """

        return set_task_strictness(task, strict or False)

    def taskable(
            self,
            taskable: Taskable | None = None,
            *,
            task_cls: typing.Optional[type[Task]] = None,
            **kwds) -> Task:
        """
        Wraps a callable object in a `Task` object
        and then registers it to this
        `TaskBroker`.
        """

        return register_task(self, taskable, task_cls=task_cls or Task, **kwds)

    def __init__(
            self,
            *,
            workers: typing.Optional[int] = None,
            debug: typing.Optional[bool] = None):

        self._registry   = {}
        self._runner     = asyncio.Runner(debug=debug)
        self._task_pool  = mpp.ThreadPool((workers or 1) + 1)
        self._task_queue = collections.deque()

        def queue_watcher():
            while True:
                time.sleep(0.1)
                if not len(self.queue):
                    continue

                result = self.queue.pop()
                if result.ready():
                    continue
                self.queue.append(result)

        self._task_pool.apply_async(queue_watcher)

    def __del__(self):
        self.shutdown()


class TaskTimeWarning(RuntimeWarning):
    ...


broker = TaskBroker(workers=10)


@broker.taskable
class SomeTaskableObject:

    async def __call__(self, *args):
        ...


@broker.broker
@broker.taskable
async def some_taskable_func(*args):
    print("hello")


broker.schedule(SomeTaskableObject)
