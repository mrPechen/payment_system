from src.messaging.consumer import decide_route
from src.handlers.processing_handlers import (
    ACTION_DLQ,
    ACTION_DONE,
    ACTION_RETRY,
    ProcessingResult,
)


def test_ack_on_done():
    assert decide_route(0, ProcessingResult(ACTION_DONE)) == ("ack", None, None)


def test_retry_advances_count():
    assert decide_route(0, ProcessingResult(ACTION_RETRY)) == ("retry", 1, None)
    assert decide_route(1, ProcessingResult(ACTION_RETRY)) == ("retry", 2, None)


def test_retry_exhausted_goes_to_dlq():
    action, count, reason = decide_route(2, ProcessingResult(ACTION_RETRY, "down"))
    assert action == "dlq"
    assert count is None
    assert reason == "down"


def test_immediate_dlq_on_poison():
    action, _, reason = decide_route(0, ProcessingResult(ACTION_DLQ, "not_found"))
    assert action == "dlq"
    assert reason == "not_found"
