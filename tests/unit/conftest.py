"""Shared fixtures for unit testing"""
from prometheus_client import REGISTRY, CollectorRegistry
import pytest

@pytest.fixture(autouse=True)
def setup():
    """
    Actions to run before and after each test
    """
    collector = CollectorRegistry(auto_describe=True)
    REGISTRY.register(collector)
    yield
    collectors = list(REGISTRY._collector_to_names.keys()) # pylint: disable=W0212
    for collector in collectors:
        REGISTRY.unregister(collector)
