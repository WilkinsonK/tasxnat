import abc, typing

Ta = typing.TypeVar("Ta")
Ta_co = typing.TypeVar("Ta_co", covariant=True)


class TaskBroker(typing.Protocol[Ta]):
    """
    Manages `Taskable` objects. This includes
    instantiation, execution and evaluation of
    execution results.
    """

    @property
    @abc.abstractmethod
    def metadata(self) -> typing.Mapping[Ta, "Taskable[Ta]"]:
        """Task metadata."""

    @typing.overload
    @abc.abstractmethod
    def task(self, fn: typing.Callable, /) -> "Taskable[Ta]":
        ...

    @typing.overload
    @abc.abstractmethod
    def task(self, **kwds) -> typing.Callable[[], "Taskable[Ta]"]:
        ...

    @abc.abstractmethod
    def task(self, fn: typing.Callable | None = None, **kwds) -> "Taskable[Ta]" | typing.Callable[[], "Taskable[Ta]"]: 
        """
        Creates and registers a `Taskable`
        object.
        """

    @abc.abstractmethod
    def register_task(self, taskable: "Taskable[Ta]") -> None:
        """
        Register a `Taskable` object to this.
        task manager.
        """

    @abc.abstractmethod
    def process_tasks(self, *task_identifiers: Ta) -> None:
        """
        Executes given tasks from their
        identifiers.
        """


class Taskable(typing.Protocol[Ta_co]):
    """
    Handles some task defined by this class.
    """

    @property
    @abc.abstractmethod
    def identifier(self) -> Ta_co:
        """Identifier of this `Taskable`."""

    @property
    @abc.abstractmethod
    def manager(self):
        """Parent `TaskManager`."""

    @property
    @abc.abstractmethod
    def is_success(self) -> bool:
        """
        Whether this task completed successfully.
        """

    @property
    @abc.abstractmethod
    def is_strict(self) -> bool:
        """
        Whether this task should cause subsequent
        tasks to fail/not execute.
        """

    @abc.abstractmethod
    def handle(self, *args, **kwds) -> None:
        """
        Executes this task with the arguments
        passed.
        """
