from pytest import fixture


@fixture
def dummy_hello_str() -> str:
    """A dummy str fixture for pytests."""
    return "hello, world"