__all__ =\
(
    "get_broker",
    "TaskConfig",
    "TaskResult",
    "Task",
    "TaskBroker",
    "TaskTimeWarning"
)

import asyncio, concurrent.futures, functools, inspect, time, typing, warnings

from tasxnat.protocols import TaskI, TaskBrokerI, TaskResultI

_Ps = typing.ParamSpec("_Ps")
_Rt = typing.TypeVar("_Rt")
_Rt_co = typing.TypeVar("_Rt_co", covariant=True)

Tasked      = typing.Callable[_Ps, _Rt]
Taskable    = typing.Callable[_Ps, _Rt] | type[typing.Callable[_Ps, _Rt]]
TaskId      = typing.Hashable
TaskWrapper = typing.Callable[[Taskable], Taskable] | typing.Callable[["Task"], "Task"]


def _caller_name(caller: typing.Any):
    name = getattr(caller, "__name__", caller.__class__.__name__)
    return ":".join([caller.__module__, name])


def _create_task(
        broker: "TaskBroker",
        taskable: Taskable | None,
        task_cls: type[typing.Any], **kwds):

    def inner(wrapped: Taskable):
        broker.register(task_cls(broker, wrapped, **kwds))
        return wrapped

    if taskable:
        return inner(taskable)
    else:
        return inner


def _is_taskable(obj: typing.Any):
    return callable(obj) and not isinstance(obj, Task)


def _set_task_strictness(broker: "TaskBroker", task: typing.Any, strict: bool):

    def inner(wrapped: Task | Taskable):
        task = broker._do_register_lookup(wrapped) #type: ignore[attr-defined]
        task.config._is_strict = strict
        return wrapped

    if task:
        return inner(task)
    else:
        return inner


def _task_repr(obj: typing.Any):
    idn = hex(id(obj))
    return f"<{obj.__class__.__qualname__}({obj.name}) at {idn}>"


def _use_task_broker(broker: "TaskBroker", task: typing.Any):

    def inner(wrapped: Task | Taskable):
        task = broker._do_register_lookup(wrapped) #type: ignore[attr-defined]
        task.config._include_broker = True
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

    def __init__(self, broker, taskable, **kwds):
        if isinstance(taskable, type) and hasattr(taskable, "__call__"):
            self._caller = taskable(**kwds)
            func = taskable.__call__
        else:
            self._caller = functools.partial(taskable, **kwds)
            func = taskable

        self._broker = broker
        self._include_broker = False
        self._is_async  = inspect.iscoroutinefunction(func)
        self._is_strict = False


class TaskResult(TaskResultI[_Rt_co]):

    @property
    def failure(self):
        return self._failure

    @property
    def failure_reason(self):
        if not self._failure:
            return
        return str(self._failure)

    @property
    def is_success(self):
        return self._failure is None

    @property
    def result(self):
        return self._result

    def __init__(self):
        self._result  = None
        self._failure = RuntimeError("Task was never handled.")


class Task(TaskI[_Ps, _Rt_co]):

    class Config(TaskConfig):
        ...

    @property
    def config(self):
        return self._config

    @property
    def name(self):
        return _caller_name(self.config.caller)

    @property
    def parent(self):
        return self.config.broker

    def handle(self, *args, **kwds):
        """Handle the wrapped callable."""

        result = TaskResult()

        if self.config.include_broker:
            args = (self.parent, *args)

        try:
            if self.config.is_async:
                rt = self._handle_async(*args, **kwds)
            else:
                rt = self._handle_synch(*args, **kwds)

            result._failure = None
            result._result = rt
        except Exception as error:
            result._failure = error

        if not result.is_success:
            if self.config.is_strict:
                raise result.failure
            else:
                message = f"{self.name} failed with message: {result.failure!s}"
                warnings.warn(message, TaskTimeWarning)

        return result

    def _handle_async(self, *args, **kwds):
        while self.parent.runner.get_loop().is_running():
            time.sleep(0.1)

        loop = self.parent.runner.get_loop()
        coro = self.config.caller(*args, **kwds)

        # We want to try and use the runner first
        # if the loop is not actively running.
        # Otherwise we will schedule the task as
        # usual.
        if not loop.is_running():
            return self.parent.runner.run(coro)

        future = concurrent.futures.Future()

        async def inner(coro, fut):
            fut.set_result(await coro)

        # Schedules the wrapper where we apply
        # the result from our caller to the
        # Future.
        loop.call_soon_threadsafe(asyncio.ensure_future, inner(coro, future))

        return future.result(10) #TODO: How to best make this dynamic?

    def _handle_synch(self, *args, **kwds):
        return self.config.caller(*args, **kwds)

    def __init__(self, broker, taskable, **kwds):
        self._config = self.Config(broker, taskable, **kwds)

    def __repr__(self):
        return _task_repr(self)


class TaskBroker(TaskBrokerI):

    @property
    def name(self):
        return self._name

    @property
    def runner(self):
        return self._runner

    def broker(self, task=None):
        return _use_task_broker(self, task)

    def register(self, task: Task):
        self._register[task.name] = task

    def schedule(self, task, *args, **kwds):
        task = self._do_register_lookup(task)
        return self._executor.submit(task.handle, *args, **kwds) #type: ignore[union-attr]

    def shutdown(self):
        self._runner.close()
        self._executor.shutdown(cancel_futures=True)

    def strict(self, task=None, *, strict=None):
        return _set_task_strictness(self, task, strict or True)

    def taskable(self, taskable=None, *, task_cls=None, **kwds):
        return _create_task(self, taskable, task_cls=task_cls or Task, **kwds)

    def wait(self, future_rt, *, timeout=None):
        gen = concurrent.futures.as_completed(future_rt, timeout=timeout)
        for result in gen:
            yield result.result()

    def _do_register_lookup(self, obj):
        name = None
        if _is_taskable(obj):
            name = _caller_name(obj)
        elif isinstance(obj, Task):
            name = obj.name
        elif isinstance(obj, str):
            name = obj

        if name:
            return self._register[name]
        raise ValueError("expected a Task or Taskable object, got", type(obj))

    def __repr__(self):
        return _task_repr(self)

    def __init__(self, *, debug=None, name=None, workers=None):
        self._executor = concurrent.futures.ThreadPoolExecutor(workers)
        self._register = dict[TaskId, Task]()
        self._runner = asyncio.Runner(debug=debug)

        # Register this broker for easier future
        # lookup.
        name = name or inspect.getmodule(inspect.stack()[1][0]).__name__
        contains_name = [k for k in _task_broker_register.keys() if name in k]
        if contains_name:
            name = f"{name}:{len(contains_name)}"
        elif name != "tasxnat.root_broker":
            name = ":".join([name, "main"])

        _task_broker_register[name] = self
        self._name = name

    def __del__(self):
        self.shutdown()


def get_broker(id=None, *, broker_cls=None, **kwds):
    if not id and (not kwds and not broker_cls):
        return _root_broker

    if id in _task_broker_register:
        return _task_broker_register[id]

    broker_cls = broker_cls or TaskBroker
    broker = broker_cls(name=id, **kwds)
    return broker


_task_broker_register: dict[typing.Hashable, TaskBrokerI] = dict()
_root_broker = TaskBroker(name="tasxnat.root_broker")


class TaskTimeWarning(RuntimeWarning):
    pass
