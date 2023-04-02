import asyncio, copy, inspect, re
import abc, typing

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
            preparsed.append(rparams[seek0:seek1].lstrip(" "))
            seek0 = seek1
            continue
    preparsed.append(rparams[seek0:seek1].lstrip(" "))

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

    @abc.abstractmethod
    def process_tasks(self, *task_callers: str) -> None:
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
                      is_strict: typing.Optional[bool]) -> typing.Self:
        """
        Create a `Taskable` from a callable
        object.
        """


class SimpleMetaData(typing.TypedDict):
    strict_mode: bool
    task_class: type[Taskable]


class SimpleTaskBroker(TaskBroker):

    __metadata__: SimpleMetaData
    __register__: dict[str, Taskable] 

    @property
    def metadata(self):
        return self.__metadata__

    def task(self, #type: ignore[override]
             fn: typing.Optional[typing.Callable] = None,
             *,
             klass: typing.Optional[type[Taskable]] = None,
             is_strict: typing.Optional[bool] = None):
        klass = klass or self.metadata["task_class"]

        def wrapper(func):
            task = klass.from_callable(self, func, is_strict)
            self.register_task(task)
            return task

        if fn:
            return wrapper(fn)
        return wrapper

    def register_task(self, taskable: "Taskable"):
        self.__register__[taskable.identifier] = taskable

    def process_tasks(self, *task_callers: str):
        strict_mode = self.metadata["strict_mode"]

        for task_call in task_callers:
            iden, args, kwds = _parse_task_call(task_call)
            task = copy.deepcopy(self.__register__[iden])
            task.handle(*args, **kwds)

            if task.is_success:
                continue

            # Bail on first failure.
            if strict_mode and task.is_strict:
                if task.failure[1]:
                    raise task.failure[1]

    @typing.overload
    def __init__(self, /):
        ...

    @typing.overload
    def __init__(self,
                 *,
                 strict_mode: typing.Optional[bool] = None,
                 task_class: typing.Optional[type[Taskable]] = None):
        ...

    def __init__(self,
                 *,
                 strict_mode: typing.Optional[bool] = None,
                 task_class: typing.Optional[type[Taskable]] = None):
        self.__metadata__ = (
            {
                "strict_mode": strict_mode or False,
                "task_class": task_class or SimpleTask
            })
        self.__register__ = {}


class SimpleTask(Taskable):
    _broker: TaskBroker
    _failure_reason: str | None
    _failure_exception: Exception | None
    _is_strict: bool
    _is_success: bool
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
    def is_strict(self):
        return self._is_strict

    @property
    def is_success(self):
        return self._is_success

    def handle(self, *args, **kwds):
        try:
            if inspect.iscoroutinefunction(self._task):
                (asyncio.get_event_loop_policy()
                    .get_event_loop()
                    .run_until_complete(self._task(*args, **kwds)))
            else:
                self._task(*args, **kwds)
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
                      is_strict: typing.Optional[bool] = None):
        return cls(broker, fn, is_strict)

    def __init__(self,
                 broker: TaskBroker,
                 fn: typing.Callable,
                 is_strict: typing.Optional[bool] = None):
        self._broker = broker
        self._failure_reason = "Task was never handled."
        self._failure_exception = None
        self._is_strict = is_strict or False
        self._is_success = False
        self._task = fn


if __name__ == "__main__":
    broker = SimpleTaskBroker(strict_mode=True)

    @broker.task(is_strict=True)
    def say_hello(name: str = "Duey", age: int = 0):
        print(f"Hello {name}! Your age is {age} years")

    @broker.task(is_strict=True)
    async def asay_hello(name: str):
        print(f"Hello {name}")

    broker.process_tasks(
        "__main__:asay_hello[Keenan]",
        "__main__:asay_hello[Ryan]",
        "__main__:asay_hello[Helen]",
        "__main__:say_hello[ '' 14]",
        "__main__:say_hello['Klayton' 17 ]",
        "__main__:say_hello[Keenan 27]",
        "__main__:say_hello[\"Lucy\" age='32']",
        "__main__:say_hello['Huey Luis']")
