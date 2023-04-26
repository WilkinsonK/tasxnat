from tasxnat.protocols import Taskable, TaskBroker, TaskedCallable
from tasxnat.objects import *
from tasxnat.objects import _simple_identifier


class TestTaskableObjects:

    def test_can_build_from_callable(self, taskable: Taskable):
        assert isinstance(taskable, Taskable),\
            f"Object {taskable!r} does not conform to 'Taskable' interface."

    def test_taskable_runs_successfully(self, taskable: Taskable):
        taskable.handle()
        assert taskable.is_success,\
            "Test Taskable is expected to run successfully."

    def test_bad_taskable_panics(self, bad_taskable: Taskable):
        try:
            bad_taskable.handle()
        except RuntimeError:
            ...

        assert isinstance(bad_taskable.failure[1], RuntimeError),\
            "Bad test Taskable is expected to throw a RuntimeError."
        assert bad_taskable.failure[0] == "This is a testing failure.",\
            f"Expected a specific failing message, got {bad_taskable.failure[0]!r}"


class TestTaskBrokerObjects:

    def test_can_build(self, task_broker: TaskBroker):
        assert isinstance(task_broker, TaskBroker),\
            f"Object {task_broker!r} does not conform to 'TaskBroker' interface."

    def test_can_register_task(self,
                               task_broker: TaskBroker,
                               taskable: Taskable):
        task_broker.register_task(taskable)

    def test_can_process_task(self,
                              task_broker: TaskBroker,
                              taskable: Taskable,
                              optsmallint):
        task_broker.register_task(taskable)

        task_broker.process_tasks(
            taskable.identifier,
            process_count=optsmallint)

    def test_bad_task_panics(self,
                             task_broker: TaskBroker,
                             bad_taskable: Taskable,
                             optsmallint):
        task_broker.register_task(bad_taskable)

        error, message = None, None
        try:
            task_broker.process_tasks(
                bad_taskable.identifier,
                process_count=optsmallint)
        except RuntimeError as e:
            error, message = e, str(e)

        assert isinstance(error, RuntimeError),\
            "Bad test Taskable is expected to throw a RuntimeError."
        assert (message == "This is a testing failure."), \
            f"Expected a specific failing message, got {message!r}"

    def test_can_push_before(self, task_broker: TaskBroker):

        def some_before_task(tasked):
            word, *args = tasked.args
            word += " Red"
            tasked.args = (word, *args)

        @task_broker.before(some_before_task)
        @task_broker.task(is_strict=True)
        def taskable_func(_, *args, **kwds):
            word, *args = args
            word += " Ridinghood"

            assert word == "Little Red Ridinghood",\
                "Expected a certain message from call response."

        identifier = _simple_identifier(taskable_func)
        task_broker.process_tasks(f"{identifier}[Little]")

    def test_can_push_after(self, task_broker: TaskBroker):

        def some_after_task(tasked: TaskedCallable):
            word, *args = tasked.args
            word += " Red"
            tasked.args = (word, *args)

        @task_broker.after(some_after_task)
        @task_broker.task(is_strict=True)
        def taskable_func(_, *args, **kwds):
            return (args, kwds)

        identifier = _simple_identifier(taskable_func)
        task_broker.process_tasks(f"{identifier}[Little]")
