# -*- test-case-name: twisted.trial._dist.test.test_disttrial -*-
# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
This module contains the trial distributed runner, the management class
responsible for coordinating all of trial's behavior at the highest level.

@since: 12.3
"""

import os
import sys

from twisted.python.filepath import FilePath
from twisted.python.modules import theSystemPath
from twisted.internet.defer import DeferredList
from twisted.internet.task import cooperate

from twisted.trial.util import _unusedTestDirectory
from twisted.trial._asyncrunner import _iterateTests
from twisted.trial._dist.worker import LocalWorker, LocalWorkerAMP
from twisted.trial._dist.distreporter import DistReporter
from twisted.trial.reporter import UncleanWarningsReporterWrapper
from twisted.trial._dist import _WORKER_AMP_STDIN, _WORKER_AMP_STDOUT



class DistTrialRunner(object):
    """
    A specialized runner for distributed trial. The runner launches a number of
    local worker processes which will run tests.

    @ivar _workerNumber: the number of workers to be spawned.
    @type _workerNumber: C{int}

    @ivar _stream: stream which the reporter will use.

    @ivar _reporterFactory: the reporter class to be used.
    """
    _distReporterFactory = DistReporter

    def _makeResult(self):
        """
        Make reporter factory, and wrap it with a L{DistReporter}.
        """
        reporter = self._reporterFactory(self._stream, self._tbformat,
                                         realtime=self._rterrors)
        if self._uncleanWarnings:
            reporter = UncleanWarningsReporterWrapper(reporter)
        return self._distReporterFactory(reporter)


    def __init__(self, reporterFactory, workerNumber, workerArguments,
                 stream=None,
                 tracebackFormat='default',
                 realTimeErrors=False,
                 uncleanWarnings=False,
                 logfile='test.log',
                 workingDirectory='_trial_temp'):
        self._workerNumber = workerNumber
        self._workerArguments = workerArguments
        self._reporterFactory = reporterFactory
        if stream is None:
            stream = sys.stdout
        self._stream = stream
        self._tbformat = tracebackFormat
        self._rterrors = realTimeErrors
        self._uncleanWarnings = uncleanWarnings
        self._result = None
        self._workingDirectory = workingDirectory
        self._logFile = logfile
        self._logFileObserver = None
        self._logFileObject = None
        self._logWarnings = False


    def writeResults(self, result):
        """
        Write test run final outcome to result.

        @param result: A C{TestResult} which will print errors and the summary.
        """
        result.done()


    def createLocalWorkers(self, protocols, workingDirectory):
        """
        Create local worker protocol instances and return them.

        @param protocols: An iterable of L{LocalWorkerAMP} instances.

        @param workingDirectory: The base path in which we should run the
            workers.
        @type workingDirectory: C{str}

        @return: A list of C{quantity} C{LocalWorker} instances.
        """
        return [LocalWorker(protocol,
                            os.path.join(workingDirectory, str(x)),
                            self._logFile)
                for x, protocol in enumerate(protocols)]


    def launchWorkerProcesses(self, spawner, protocols, arguments):
        """
        Spawn processes from a list of process protocols.

        @param spawner: A C{IReactorProcess.spawnProcess} implementation.

        @param protocols: An iterable of C{ProcessProtocol} instances.

        @param arguments: Extra arguments passed to the processes.
        """
        workertrialPath = theSystemPath[
            'twisted.trial._dist.workertrial'].filePath.path
        childFDs = {0: 'w', 1: 'r', 2: 'r', _WORKER_AMP_STDIN: 'w',
                    _WORKER_AMP_STDOUT: 'r'}
        environ = os.environ.copy()
        # Add an environment variable containing the raw sys.path, to be used by
        # subprocesses to make sure it's identical to the parent. See
        # workertrial._setupPath.
        environ['TRIAL_PYTHONPATH'] = os.pathsep.join(sys.path)
        for worker in protocols:
            args = [sys.executable, workertrialPath]
            args.extend(arguments)
            spawner(worker, sys.executable, args=args, childFDs=childFDs,
                    env=environ)


    def _driveWorker(self, worker, result, testCases, cooperate):
        """
        Drive a L{LocalWorkerAMP} instance, iterating the tests and calling
        C{run} for every one of them.

        @param worker: The L{LocalWorkerAMP} to drive.

        @param result: The global L{DistReporter} instance.

        @param testCases: The global list of tests to iterate.

        @param cooperate: The cooperate function to use, to be customized in
            tests.
        @type cooperate: C{function}

        @return: A C{Deferred} firing when all the tests are finished.
        """

        def resultErrback(error, case):
            result.original.addFailure(case, error)
            return error

        def task(case):
            d = worker.run(case, result)
            d.addErrback(resultErrback, case)
            return d

        return cooperate(task(case) for case in testCases).whenDone()


    def run(self, suite, reactor=None, cooperate=cooperate,
            untilFailure=False):
        """
        Spawn local worker processes and load tests. After that, run them.

        @param suite: A tests suite to be run.

        @param reactor: The reactor to use, to be customized in tests.
        @type reactor: A provider of
            L{twisted.internet.interfaces.IReactorProcess}

        @param cooperate: The cooperate function to use, to be customized in
            tests.
        @type cooperate: C{function}

        @param untilFailure: If C{True}, continue to run the tests until they
            fail.
        @type untilFailure: C{bool}.

        @return: The test result.
        @rtype: L{DistReporter}
        """
        if reactor is None:
            from twisted.internet import reactor
        result = self._makeResult()
        count = suite.countTestCases()
        self._stream.write("Running %d tests.\n" % (count,))

        if not count:
            # Take a shortcut if there is no test
            suite.run(result.original)
            self.writeResults(result)
            return result

        testDir, testDirLock = _unusedTestDirectory(
            FilePath(self._workingDirectory))
        workerNumber = min(count, self._workerNumber)
        ampWorkers = [LocalWorkerAMP() for x in range(workerNumber)]
        workers = self.createLocalWorkers(ampWorkers, testDir.path)
        processEndDeferreds = [worker.endDeferred for worker in workers]
        self.launchWorkerProcesses(reactor.spawnProcess, workers,
                                   self._workerArguments)

        def runTests():
            testCases = iter(list(_iterateTests(suite)))

            workerDeferreds = []
            for worker in ampWorkers:
                workerDeferreds.append(
                    self._driveWorker(worker, result, testCases,
                                      cooperate=cooperate))
            return DeferredList(workerDeferreds, consumeErrors=True,
                                fireOnOneErrback=True)

        stopping = []

        def nextRun(ign):
            self.writeResults(result)
            if not untilFailure:
                return
            if not result.wasSuccessful():
                return
            d = runTests()
            return d.addCallback(nextRun)

        def stop(ign):
            testDirLock.unlock()
            if not stopping:
                stopping.append(None)
                reactor.stop()

        def beforeShutDown():
            if not stopping:
                stopping.append(None)
                d = DeferredList(processEndDeferreds, consumeErrors=True)
                return d.addCallback(continueShutdown)

        def continueShutdown(ign):
            self.writeResults(result)
            return ign

        d = runTests()
        d.addCallback(nextRun)
        d.addBoth(stop)

        reactor.addSystemEventTrigger('before', 'shutdown', beforeShutDown)
        reactor.run()

        return result


    def runUntilFailure(self, suite):
        """
        Run the tests with local worker processes until they fail.

        @param suite: A tests suite to be run.
        """
        return self.run(suite, untilFailure=True)
