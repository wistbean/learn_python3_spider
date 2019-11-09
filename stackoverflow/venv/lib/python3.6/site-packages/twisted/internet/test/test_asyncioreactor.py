# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Tests for L{twisted.internet.asyncioreactor}.
"""

from twisted.trial.unittest import SynchronousTestCase
from .reactormixins import ReactorBuilder

try:
    from twisted.internet.asyncioreactor import AsyncioSelectorReactor
    import asyncio
except ImportError:
    AsyncioSelectorReactor = None
    skipReason = "Requires asyncio."



class AsyncioSelectorReactorTests(ReactorBuilder, SynchronousTestCase):
    """
    L{AsyncioSelectorReactor} tests.
    """
    if AsyncioSelectorReactor is None:
        skip = skipReason


    def test_defaultEventLoopFromGlobalPolicy(self):
        """
        L{AsyncioSelectorReactor} wraps the global policy's event loop
        by default.  This ensures that L{asyncio.Future}s and
        coroutines created by library code that uses
        L{asyncio.get_event_loop} are bound to the same loop.
        """
        reactor = AsyncioSelectorReactor()
        future = asyncio.Future()
        result = []

        def completed(future):
            result.append(future.result())
            reactor.stop()

        future.add_done_callback(completed)
        future.set_result(True)

        self.assertEqual(result, [])
        self.runReactor(reactor, timeout=1)
        self.assertEqual(result, [True])
