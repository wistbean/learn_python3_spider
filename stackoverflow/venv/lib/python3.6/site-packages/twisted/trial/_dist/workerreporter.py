# -*- test-case-name: twisted.trial._dist.test.test_workerreporter -*-
#
# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Test reporter forwarding test results over trial distributed AMP commands.

@since: 12.3
"""

from twisted.python.failure import Failure
from twisted.python.reflect import qual
from twisted.trial.reporter import TestResult
from twisted.trial._dist import managercommands



class WorkerReporter(TestResult):
    """
    Reporter for trial's distributed workers. We send things not through a
    stream, but through an C{AMP} protocol's C{callRemote} method.

    @ivar _DEFAULT_TODO: Default message for expected failures and
        unexpected successes, used only if a C{Todo} is not provided.
    """

    _DEFAULT_TODO = 'Test expected to fail'

    def __init__(self, ampProtocol):
        """
        @param ampProtocol: The communication channel with the trial
            distributed manager which collects all test results.
        @type ampProtocol: C{AMP}
        """
        super(WorkerReporter, self).__init__()
        self.ampProtocol = ampProtocol


    def _getFailure(self, error):
        """
        Convert a C{sys.exc_info()}-style tuple to a L{Failure}, if necessary.
        """
        if isinstance(error, tuple):
            return Failure(error[1], error[0], error[2])
        return error


    def _getFrames(self, failure):
        """
        Extract frames from a C{Failure} instance.
        """
        frames = []
        for frame in failure.frames:
            frames.extend([frame[0], frame[1], str(frame[2])])
        return frames


    def addSuccess(self, test):
        """
        Send a success over.
        """
        super(WorkerReporter, self).addSuccess(test)
        testName = test.id()
        self.ampProtocol.callRemote(managercommands.AddSuccess,
                                    testName=testName)


    def addError(self, test, error):
        """
        Send an error over.
        """
        super(WorkerReporter, self).addError(test, error)
        testName = test.id()
        failure = self._getFailure(error)
        error = failure.getErrorMessage()
        errorClass = qual(failure.type)
        frames = [frame for frame in self._getFrames(failure)]
        self.ampProtocol.callRemote(managercommands.AddError,
                                    testName=testName,
                                    error=error,
                                    errorClass=errorClass,
                                    frames=frames)


    def addFailure(self, test, fail):
        """
        Send a Failure over.
        """
        super(WorkerReporter, self).addFailure(test, fail)
        testName = test.id()
        failure = self._getFailure(fail)
        fail = failure.getErrorMessage()
        failClass = qual(failure.type)
        frames = [frame for frame in self._getFrames(failure)]
        self.ampProtocol.callRemote(managercommands.AddFailure,
                                    testName=testName,
                                    fail=fail,
                                    failClass=failClass,
                                    frames=frames)


    def addSkip(self, test, reason):
        """
        Send a skip over.
        """
        super(WorkerReporter, self).addSkip(test, reason)
        reason = str(reason)
        testName = test.id()
        self.ampProtocol.callRemote(managercommands.AddSkip,
                                    testName=testName,
                                    reason=reason)


    def _getTodoReason(self, todo):
        """
        Get the reason for a C{Todo}.

        If C{todo} is L{None}, return a sensible default.
        """
        if todo is None:
            return self._DEFAULT_TODO
        else:
            return todo.reason


    def addExpectedFailure(self, test, error, todo=None):
        """
        Send an expected failure over.
        """
        super(WorkerReporter, self).addExpectedFailure(test, error, todo)
        errorMessage = error.getErrorMessage()
        testName = test.id()
        self.ampProtocol.callRemote(managercommands.AddExpectedFailure,
                                    testName=testName,
                                    error=errorMessage,
                                    todo=self._getTodoReason(todo))


    def addUnexpectedSuccess(self, test, todo=None):
        """
        Send an unexpected success over.
        """
        super(WorkerReporter, self).addUnexpectedSuccess(test, todo)
        testName = test.id()
        self.ampProtocol.callRemote(managercommands.AddUnexpectedSuccess,
                                    testName=testName,
                                    todo=self._getTodoReason(todo))


    def printSummary(self):
        """
        I{Don't} print a summary
        """
