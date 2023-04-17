import pytest

from tasxnat.objects import SimpleTaskable
from tasxnat.utilities import *


@pytest.fixture(params=[True, False, None])
def optbool1(request) -> bool | None:
    return request.param


@pytest.fixture(params=[True, False, None])
def optbool2(request) -> bool | None:
    return request.param


@pytest.fixture(params=[None, 0, 1, 2, 4])
def optsmallint(request) -> int | None:
    return request.param


@pytest.fixture(
    params=[
        "assets:asay_hello[Keenan]",
        "assets:asay_hello[Ryan]",
        "assets:asay_hello[Helen]",
        "package.assets:say_hello[ '' 14]",
        "assets:say_hello['Klayton' 17 ]",
        "assets:say_hello[Keenan 27]",
        "assets:say_hello[\"Lucy\" age='32']",
        "assets:say_hello['Huey Luis']",
        "assets:delay_greet[ Amber]",
        "assets:delay_greet[ Quentin ]",
        "assets:delay_greet[Yuri]",
        "assets:delay_greet[Cosmo]",
        "assets:delay_greet[Wanda]",
        "assets:empty_params"
])
def task_call(request):
    return request.param


@pytest.fixture
def parsed_task_call(task_call):
    return _parse_task_call(task_call)


@pytest.fixture(params=[True, False])
def taskable_callable(request):

    def example_func(*args, **kwds):
        ...

    async def example_async_func(*args, **kwds):
        ...

    if request.param:
        return example_func
    return example_async_func


@pytest.fixture
def taskable(taskable_callable, optsmallint, optbool1, optbool2):
    return SimpleTaskable.from_callable(
        taskable_callable,
        optsmallint,
        optbool1,
        optbool2)
