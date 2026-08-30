from app.queue.models import JobStatus
from app.queue.retry_policy import RetryPolicy


def test_job_status_contains_paused():
    assert JobStatus.PAUSED.value == 'paused'


def test_retry_policy_exponential_cap_and_exhaustion():
    policy = RetryPolicy(max_attempts=3, base_seconds=300, max_seconds=1000)
    assert [policy.delay(i) for i in (1, 2, 3, 4)] == [300, 600, 1000, 1000]
    assert not policy.exhausted(2)
    assert policy.exhausted(3)
