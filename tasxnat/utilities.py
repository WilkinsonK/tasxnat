import asyncio, copy, inspect, re
import typing
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

from tasxnat.protocols import Taskable, TaskQueue


__all__ = (
    (
        "_parse_task_call",
        "_flatten_to_taskmaps",
        "_handle_coroutine",
        "_process_tasks",
        "_process_tasks_multi"
    ))

_RE_TASK_CALLER = re.compile(r"^[\w\.\:]+|\[.+\]$")


#NOTE: this is fairly lazy, let alone a 'dumb'
# algorithm, but will work for now.
#TODO: find better parsing solution.
def _parse_task_call(task_call: str) -> tuple[str, tuple[str, ...], dict[str, str]]:
    found = _RE_TASK_CALLER.findall(task_call)
    if len(found) == 2:
        caller, rparams = found
    else:
        caller, rparams = found[0], ""

    rparams = rparams.lstrip("[ ").rstrip(" ]")
    if not len(rparams):
        return caller, (), {} #type: ignore[return-value]
    rparams += "\0"

    preparsed, in_quotes = list[str](), False
    seek0, seek1 = 0, 0
    while rparams[seek1] != "\0":
        if rparams[seek1] in ("'", "\""):
            in_quotes = not in_quotes

        seek1 += 1
        if rparams[seek1] == " " and not in_quotes:
            preparsed.append(rparams[seek0:seek1].lstrip())
            seek0 = seek1
            continue
    preparsed.append(rparams[seek0:seek1].lstrip())

    args, kwds = (), {} #type: ignore[annotated]
    for rparam in preparsed:
        # We don't allow implicit empty values.
        # Empty strings are represented as quoted
        # strings.
        if not rparam:
            raise ValueError(f"Illegal implicit empty string.")

        if "=" not in rparam:
            # Remove quotes from param.
            args += (rparam.strip("'\" "),) #type: ignore[assignment]
        else:
            k, v = rparam.split("=", maxsplit=1)

            if not v:
                raise ValueError(f"Illegal implicit empty string.")
            kwds[k] = v.strip("'\" ")

    return caller, args, kwds #type: ignore[return-value]


def _flatten_to_taskmaps(
        *task_calls: str) -> list[tuple[str, typing.Iterable[tuple[tuple, dict]]]]:
    """
    Parses the given task calls grouping callargs
    with their task name. This makes it so all
    similar task calls are grouped together.
    """

    # Collect all task calls in groups to
    # process similar calls together.
    taskable_map = dict[str, tuple]()
    for task_call in task_calls:
        iden, *callargs = _parse_task_call(task_call)
        if iden in taskable_map:
            taskable_map[iden] += (callargs,)
        else:
            taskable_map[iden] = (callargs,)

    # Flatten the taskable map for pool
    # consumption
    return [(iden, calls) for iden, calls in taskable_map.items()]


def _handle_coroutine(
        coro: typing.Coroutine,
        multithread_mode: bool | None = None):
    policy = asyncio.get_event_loop_policy()
    try:
        loop = policy.get_event_loop()
    except RuntimeError:
        loop = policy.new_event_loop()

    ret = loop.run_until_complete(coro)
    if multithread_mode:
        # Closing the loop after running the
        # coroutine. Should prevent issues when
        # loop.__del__ is called.
        loop.close()

    return ret


def _process_tasks(
        root_task: Taskable,
        calls: typing.Iterable[tuple[tuple, dict]],
        strict_mode: bool):

    for args, kwds in calls:
        task = copy.deepcopy(root_task)
        task.handle(*args, **kwds)

        if task.is_success:
            continue

        # Bail on first failure if strict mode.
        if strict_mode and task.is_strict:
            if task.failure[1]:
                raise task.failure[1]


def _process_tasks_multi(
        root_task: Taskable,
        calls: typing.Iterable[tuple[tuple, dict]],
        strict_mode: bool):
    # loop = asyncio.get_event_loop_policy().get_event_loop()

    def inner(*args, **kwds):
        task = copy.deepcopy(root_task)
        task.set_thread_pool(tpool, tqueue)
        task.handle(*args, **kwds)

        if task.is_success:
            return

        if strict_mode and task.is_strict:
            _, err = task.failure
            raise err #type: ignore[misc]

    tpool  = ThreadPoolExecutor(root_task.thread_count, root_task.identifier)
    tqueue = TaskQueue([(inner, c) for c in calls], root_task.thread_count) #type: ignore[misc]

    with tpool:
        while True:

            next_calls = []
            while len(tqueue):
                next_calls.append(tqueue.pop())

            submitted = []
            for fn, callargs in next_calls:
                # Transform callable if it is a
                # coroutine.
                if inspect.iscoroutinefunction(fn):
                    callargs = ((fn(*callargs[0], **callargs[1]), True), {})
                    fn = _handle_coroutine

                # Submit function call to executor.
                submitted.append(tpool.submit(fn, *callargs[0], **callargs[1]))

            # Await results from futures.
            results = futures.wait(submitted, 30, "FIRST_EXCEPTION")
            for result in results.done:
                result.result()

            if not len(tqueue):
                break
