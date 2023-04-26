"""
Simple tasking interface. Objects in this package
are used to broker calls to registered tasks.
"""

__all__ =\
(
    "Taskable",
    "TaskBroker",
    "SimpleTaskable",
    "SimpleTaskBroker",
    "SimpleTaskedCallable",
    "AsyncTaskedCallable"
)
__version__ = (0, 0, 8)

from tasxnat.protocols import Taskable, TaskBroker
from tasxnat.objects import\
(
    SimpleTaskable,
    SimpleTaskBroker,
    SimpleTaskedCallable,
    AsyncTaskedCallable
)
