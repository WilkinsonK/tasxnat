import pytest

from tasxnat.protocols import Taskable
from tasxnat.objects import *


class TestTaskableObjects:

    def test_can_build_from_callable(self, taskable: Taskable):
        assert isinstance(taskable, Taskable), f"Object {taskable!r} does not conform to 'Taskable' interface."

    def test_taskable_runs_successfully(self, taskable: Taskable):
        taskable.handle()
        assert taskable.is_success, "Test Taskable is expected to run successfully."

    def test_bad_taskable_panics(self, bad_taskable: Taskable):
        try:
            bad_taskable.handle()
        except RuntimeError:
            ...

        assert isinstance(bad_taskable.failure[1], RuntimeError), "Bad test Taskable is expected to fail throwing an error."
        assert bad_taskable.failure[0] == "This is a testing failure.", f"Expected a specific failing message, got {bad_taskable.failure[0]!r}"
