import pytest


def pytest_addoption(parser):
    parser.addoption('--data-collector-label', action='append')


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    outcome = yield
    result = outcome.get_result()
    setattr(item, 'result', result)


@pytest.fixture
def sch(sch_session, request):
    data_collector_labels = request.config.getoption('data_collector_label') or []
    sch_session.data_collector_labels = data_collector_labels
    yield sch_session
    if request.node.result.passed:
        sch_session.delete_job(sch_session._test_job_queue.pop())
