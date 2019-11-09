# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Tests for L{twisted.trial._dist.distreporter}.
"""

from twisted.python.compat import NativeStringIO as StringIO
from twisted.trial._dist.distreporter import DistReporter
from twisted.trial.unittest import TestCase
from twisted.trial.reporter import TreeReporter



class DistReporterTests(TestCase):
    """
    Tests for L{DistReporter}.
    """

    def setUp(self):
        self.stream = StringIO()
        self.distReporter = DistReporter(TreeReporter(self.stream))
        self.test = TestCase()


    def test_startSuccessStop(self):
        """
        Success output only gets sent to the stream after the test has stopped.
        """
        self.distReporter.startTest(self.test)
        self.assertEqual(self.stream.getvalue(), "")
        self.distReporter.addSuccess(self.test)
        self.assertEqual(self.stream.getvalue(), "")
        self.distReporter.stopTest(self.test)
        self.assertNotEqual(self.stream.getvalue(), "")


    def test_startErrorStop(self):
        """
        Error output only gets sent to the stream after the test has stopped.
        """
        self.distReporter.startTest(self.test)
        self.assertEqual(self.stream.getvalue(), "")
        self.distReporter.addError(self.test, "error")
        self.assertEqual(self.stream.getvalue(), "")
        self.distReporter.stopTest(self.test)
        self.assertNotEqual(self.stream.getvalue(), "")


    def test_forwardedMethods(self):
        """
        Calling methods of L{DistReporter} add calls to the running queue of
        the test.
        """
        self.distReporter.startTest(self.test)
        self.distReporter.addFailure(self.test, "foo")
        self.distReporter.addError(self.test, "bar")
        self.distReporter.addSkip(self.test, "egg")
        self.distReporter.addUnexpectedSuccess(self.test, "spam")
        self.distReporter.addExpectedFailure(self.test, "err", "foo")
        self.assertEqual(len(self.distReporter.running[self.test.id()]), 6)
