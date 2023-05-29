__all__ =\
(
    "TaskI",
    "TaskBrokerI",
    "TaskResultI"
)

import typing

_Ps = typing.ParamSpec("_Ps")
_Rt_co = typing.TypeVar("_Rt_co", covariant=True)


class TaskI(typing.Protocol[_Ps, _Rt_co]):
    pass


class TaskBrokerI(typing.Protocol):
    pass


class TaskResultI(typing.Protocol[_Rt_co]):
    pass
