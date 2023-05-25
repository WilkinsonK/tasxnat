"""
Simple tasking interface. Objects in this package
are used to broker calls to registered tasks.
"""

__all__ =\
(
    "get_broker",
    "TaskConfig",
    "TaskResult",
    "Task",
    "TaskBroker",
    "TaskTimeWarning"
)
__version__ = (1, 0, 0)

from tasxnat.objects import\
(
    get_broker,
    TaskConfig,
    TaskResult,
    Task,
    TaskBroker,
    TaskTimeWarning
)
