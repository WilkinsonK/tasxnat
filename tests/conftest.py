import pytest

from tasxnat.utilities import *

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


@pytest.fixture(autouse=True)
def parsed_task_call(task_call):
    return _parse_task_call(task_call)
