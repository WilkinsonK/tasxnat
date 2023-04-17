import string

import pytest


class TestTaskCallParsing:

    def test_call_identity_clean(
            self,
            task_call: tuple[str, tuple[str, ...], dict[str, str]]):

        iden, *_ = task_call

        assert "\0" not in iden, "Null termination should be cleaned from identity."
        assert not any([
            ch in iden for ch in string.punctuation.replace(".", "")]), "Punctuation not allowed in task identity."
        assert not any([
            ch in iden for ch in string.whitespace]), "Whitespace not allowed in task identity."

    def test_call_args_clean(
            self,
            parsed_task_call: tuple[str, tuple[str, ...], dict[str, str]]):
        
        _, args, _ = parsed_task_call

        assert not any(['\0' in arg for arg in args]), "Null termination should be cleaned from arguments."
        
        def bad_quotation(arg: str):
            return not (arg.count(arg) % 2)

        assert not any([bad_quotation(arg) for arg in args]), "Quotations should close as expected."

    def test_call_kwds_clean(
            self,
            parsed_task_call: tuple[str, tuple[str, ...], dict[str, str]]):
        
        *_, kwds = parsed_task_call

        assert not any(['\0' in val for val in kwds.values()]), "Null termination should be cleaned from keyword arguments."

        # Only valid in this context.
        # Users should be capable of inserting
        # any characters they wish into a value
        # provided it is input properly.
        assert not any(['=' in val for val in kwds.values()]), "'=' should be cleaned from values"
        assert not any(['=' in key for key in kwds.keys()]), "'=' should be cleaned from keywords"
        assert not any([val in key for key, val in kwds.items()]), "Values should not found in keywords."
        assert not any([key in val for key, val in kwds.items()]), "Keywords should not found in values."
