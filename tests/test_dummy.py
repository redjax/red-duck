from __future__ import annotations

from pytest import mark, xfail

@mark.hello
def test_say_hello(dummy_hello_str: str):
    assert isinstance(
        dummy_hello_str, str
    ), f"Invalid test output type: ({type(dummy_hello_str)}). Should be of type str"
    assert (
        dummy_hello_str == "hello, world"
    ), f"String should have been 'hello, world', not '{dummy_hello_str}'"

    print(f"Hello, {dummy_hello_str}!")


@mark.always_pass
def test_pass():
    assert True, "test_pass() should have been True"


@mark.xfail
def test_fail():
    test_pass = False
    assert test_pass, "This test is designed to fail"