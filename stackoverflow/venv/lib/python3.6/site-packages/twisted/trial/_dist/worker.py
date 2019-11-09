# -*- test-case-name: twisted.trial._dist.test.test_worker -*-
#
# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
This module implements the worker classes.

@since: 12.3
"""

import os

from zope.interface import implementer

from twisted.internet.protocol import ProcessProtocol
from twisted.internet.interfaces import ITransport, IAddress
from twisted.internet.defer import Deferred
from twisted.protocols.amp import AMP
from twisted.python.failure import Failure
from twisted.python.reflect import namedObject
from twisted.trial.unittest import Todo
from twisted.trial.runner import TrialSuite, TestLoader
from twisted.trial._dist import workercommands, managercommands
from twisted.trial._dist import _WORKER_AMP_STDIN, _WORKER_AMP_STDOUT
from twisted.trial._dist.workerreporter import WorkerReporter



class WorkerProtocol(AMP):
    """
    The worker-side trial distributed protocol.
    """

    def __init__(self, forceGarbageCollection=False):
        self._loader = TestLoader()
        self._result = WorkerReporter(self)
        self._forceGarbageCollection = forceGarbageCollection


    def run(self, testCase):
        """
        Run a test case by name.
        """
        case = self._loader.loadByName(testCase)
        suite = TrialSuite([case], self._forceGarbageCollection)
        suite.run(self._result)
        return {'success': True}

    workercommands.Run.responder(run)


    def start(self, directory):
        """
        Set up the worker, moving into given directory for tests to run in
        them.
        """
        os.chdir(directory)
        return {'success': True}

    workercommands.Start.responder(start)



class LocalWorkerAMP(AMP):
    """
    Local implementation of the manager commands.
    """

    def addSuccess(self, testName):
        """
        Add a success to the reporter.
        """
        self._result.addSuccess(self._testCase)
        return {'success': True}

    managercommands.AddSuccess.responder(addSuccess)


    def _buildFailure(self, error, errorClass, frames):
        """
        Helper to build a C{Failure} with some traceback.

        @param error: An C{Exception} instance.

        @param error: The class name of the C{error} class.

        @param frames: A flat list of strings representing the information need
            to approximatively rebuild C{Failure} frames.

        @return: A L{Failure} instance with enough information about a test
           error.
        """
        errorType = namedObject(errorClass)
        failure = Failure(error, errorType)
        for i in range(0, len(frames), 3):
            failure.frames.append(
                (frames[i], frames[i + 1], int(frames[i + 2]), [], []))
        return failure


    def addError(self, testName, error, errorClass, frames):
        """
        Add an error to the reporter.
        """
        failure = self._buildFailure(error, errorClass, frames)
        self._result.addError(self._testCase, failure)
        return {'success': True}

    managercommands.AddError.responder(addError)


    def addFailure(self, testName, fail, failClass, frames):
        """
        Add a failure to the reporter.
        """
        failure = self._buildFailure(fail, failClass, frames)
        self._result.addFailure(self._testCase, failure)
        return {'success': True}

    managercommands.AddFailure.responder(addFailure)


    def addSkip(self, testName, reason):
        """
        Add a skip to the reporter.
        """
        self._result.addSkip(self._testCase, reason)
        return {'success': True}

    managercommands.AddSkip.responder(addSkip)


    def addExpectedFailure(self, testName, error, todo):
        """
        Add an expected failure to the reporter.
        """
        _todo = Todo(todo)
        self._result.addExpectedFailure(self._testCase, error, _todo)
        return {'success': True}

    managercommands.AddExpectedFailure.responder(addExpectedFailure)


    def addUnexpectedSuccess(self, testName, todo):
        """
        Add an unexpected success to the reporter.
        """
        self._result.addUnexpectedSuccess(self._testCase, todo)
        return {'success': True}

    managercommands.AddUnexpectedSuccess.responder(addUnexpectedSuccess)


    def testWrite(self, out):
        """
        Print test output from the worker.
        """
        self._testStream.write(out + '\n')
        self._testStream.flush()
        return {'success': True}

    managercommands.TestWrite.responder(testWrite)


    def _stopTest(self, result):
        """
        Stop the current running test case, forwarding the result.
        """
        self._result.stopTest(self._testCase)
        return result


    def run(self, testCase, result):
        """
        Run a test.
        """
        self._testCase = testCase
        self._result = result
        self._result.startTest(testCase)
        testCaseId = testCase.id()
        d = self.callRemote(workercommands.Run, testCase=testCaseId)
        return d.addCallback(self._stopTest)


    def setTestStream(self, stream):
        """
        Set the stream used to log output from tests.
        """
        self._testStream = stream



@implementer(IAddress)
class LocalWorkerAddress(object):
    """
    A L{IAddress} implementation meant to provide stub addresses for
    L{ITransport.getPeer} and L{ITransport.getHost}.
    """



@implementer(ITransport)
class LocalWorkerTransport(object):
    """
    A stub transport implementation used to support L{AMP} over a
    L{ProcessProtocol} transport.
    """

    def __init__(self, transport):
        self._transport = transport


    def write(self, data):
        """
        Forward data to transport.
        """
        self._transport.writeToChild(_WORKER_AMP_STDIN, data)


    def writeSequence(self, sequence):
        """
        Emulate C{writeSequence} by iterating data in the C{sequence}.
        """
        for data in sequence:
            self._transport.writeToChild(_WORKER_AMP_STDIN, data)


    def loseConnection(self):
        """
        Closes the transport.
        """
        self._transport.loseConnection()


    def getHost(self):
        """
        Return a L{LocalWorkerAddress} instance.
        """
        return LocalWorkerAddress()


    def getPeer(self):
        """
        Return a L{LocalWorkerAddress} instance.
        """
        return LocalWorkerAddress()



class LocalWorker(ProcessProtocol):
    """
    Local process worker protocol. This worker runs as a local process and
    communicates via stdin/out.

    @ivar _ampProtocol: The L{AMP} protocol instance used to communicate with
        the worker.

    @ivar _logDirectory: The directory where logs will reside.

    @ivar _logFile: The name of the main log file for tests output.
    """

    def __init__(self, ampProtocol, logDirectory, logFile):
        self._ampProtocol = ampProtocol
        self._logDirectory = logDirectory
        self._logFile = logFile
        self.endDeferred = Deferred()


    def connectionMade(self):
        """
        When connection is made, create the AMP protocol instance.
        """
        self._ampProtocol.makeConnection(LocalWorkerTransport(self.transport))
        if not os.path.exists(self._logDirectory):
            os.makedirs(self._logDirectory)
        self._outLog = open(os.path.join(self._logDirectory, 'out.log'), 'wb')
        self._errLog = open(os.path.join(self._logDirectory, 'err.log'), 'wb')
        self._testLog = open(
            os.path.join(self._logDirectory, self._logFile), 'w')
        self._ampProtocol.setTestStream(self._testLog)
        logDirectory = self._logDirectory
        d = self._ampProtocol.callRemote(workercommands.Start,
                                         directory=logDirectory)
        # Ignore the potential errors, the test suite will fail properly and it
        # would just print garbage.
        d.addErrback(lambda x: None)


    def connectionLost(self, reason):
        """
        On connection lost, close the log files that we're managing for stdin
        and stdout.
        """
        self._outLog.close()
        self._errLog.close()
        self._testLog.close()


    def processEnded(self, reason):
        """
        When the process closes, call C{connectionLost} for cleanup purposes
        and forward the information to the C{_ampProtocol}.
        """
        self.connectionLost(reason)
        self._ampProtocol.connectionLost(reason)
        self.endDeferred.callback(reason)


    def outReceived(self, data):
        """
        Send data received from stdout to log.
        """

        self._outLog.write(data)


    def errReceived(self, data):
        """
        Write error data to log.
        """
        self._errLog.write(data)


    def childDataReceived(self, childFD, data):
        """
        Handle data received on the specific pipe for the C{_ampProtocol}.
        """
        if childFD == _WORKER_AMP_STDOUT:
            self._ampProtocol.dataReceived(data)
        else:
            ProcessProtocol.childDataReceived(self, childFD, data)
