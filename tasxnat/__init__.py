"""
Simple tasking interface. Objects in this package
are used to broker calls to registered tasks.
"""

__all__ =\
(
    "TaskConfig",
    "TaskResult",
    "Task",
    "TaskBroker",
    "TaskTimeWarning"
)
__version__ = (1, 0, 0)

from tasxnat.objects import\
(
    TaskConfig,
    TaskResult,
    Task,
    TaskBroker,
    TaskTimeWarning
)
