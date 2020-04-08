import pytest


def pytest_addoption(parser):
    parser.addoption('--data-collector-label', action='append')

@pytest.fixture(scope='session')
def sch(sch_session, request):
    data_collector_labels = request.config.getoption('data_collector_label') or []
    sch_session.data_collector_labels = data_collector_labels
    yield sch_session
