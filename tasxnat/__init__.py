"""
Simple tasking interface. Objects in this package
are used to broker calls to registered tasks.
"""

__all__ =\
(
    # Interface Protocols.
    "TaskI",
    "TaskBrokerI",
    "TaskResultI",

    # Objects and package API.
    "get_broker",
    "TaskConfig",
    "TaskResult",
    "Task",
    "TaskBroker",
    "TaskTimeWarning"
)
__version__ = (1, 0, 0)

from tasxnat.protocols import\
(
    TaskI,
    TaskBrokerI,
    TaskResultI
)
from tasxnat.objects import\
(
    get_broker,
    TaskConfig,
    TaskResult,
    Task,
    TaskBroker,
    TaskTimeWarning
)
