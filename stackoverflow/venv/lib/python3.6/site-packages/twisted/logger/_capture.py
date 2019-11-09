# -*- test-case-name: twisted.logger.test.test_capture -*-
# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Context manager for capturing logs.
"""

from contextlib import contextmanager

from twisted.logger import globalLogPublisher



@contextmanager
def capturedLogs():
    events = []
    observer = events.append

    globalLogPublisher.addObserver(observer)

    yield events

    globalLogPublisher.removeObserver(observer)
