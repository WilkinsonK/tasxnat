import pytest

from tasxnat.protocols import Taskable
from tasxnat.objects import *


class TestTaskableObjects:

    def test_can_build_from_callable(self, taskable):
        assert isinstance(taskable, Taskable), "task returned is does not conform to 'Taskable' interface."

    def test_can_build_from_async_callable(self, taskable):
        assert isinstance(taskable, Taskable), "task returned is does not conform to 'Taskable' interface."
