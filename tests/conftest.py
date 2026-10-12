import pytest


@pytest.fixture(scope="session")
def spark():
    """One local Spark session for the whole run (with the Kafka connector, which integration tests need)."""
    from common.spark import session

    s = session("harbourwatch-tests", master="local[2]", kafka=True)
    yield s
    s.stop()
