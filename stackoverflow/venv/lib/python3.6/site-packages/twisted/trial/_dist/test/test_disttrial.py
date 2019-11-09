# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Tests for L{twisted.trial._dist.disttrial}.
"""

import os
import sys

from twisted.internet.protocol import Protocol, ProcessProtocol
from twisted.internet.defer import fail, gatherResults, maybeDeferred, succeed
from twisted.internet.task import Cooperator, deferLater
from twisted.internet.main import CONNECTION_DONE
from twisted.internet import reactor, interfaces, error
from twisted.python.compat import NativeStringIO as StringIO
from twisted.python.failure import Failure
from twisted.python.lockfile import FilesystemLock

from twisted.test.test_cooperator import FakeScheduler
from twisted.test.proto_helpers import MemoryReactorClock

from twisted.trial.unittest import SynchronousTestCase, TestCase
from twisted.trial.reporter import Reporter, TreeReporter
from twisted.trial.reporter import UncleanWarningsReporterWrapper
from twisted.trial.runner import TrialSuite, ErrorHolder

from twisted.trial._dist.disttrial import DistTrialRunner
from twisted.trial._dist.distreporter import DistReporter
from twisted.trial._dist.worker import LocalWorker

from zope.interface import implementer, verify



class FakeTransport(object):
    """
    A simple fake process transport.
    """

    def writeToChild(self, fd, data):
        """
        Ignore write calls.
        """



@implementer(interfaces.IReactorProcess)
class CountingReactor(MemoryReactorClock):
    """
    A fake reactor that counts the calls to L{IReactorCore.run},
    L{IReactorCore.stop}, and L{IReactorProcess.spawnProcess}.
    """
    spawnCount = 0
    stopCount = 0
    runCount = 0

    def __init__(self, workers):
        MemoryReactorClock.__init__(self)
        self._workers = workers


    def spawnProcess(self, worker, *args, **kwargs):
        """
        See L{IReactorProcess.spawnProcess}.

        @param worker: See L{IReactorProcess.spawnProcess}.
        @param args: See L{IReactorProcess.spawnProcess}.
        @param kwargs: See L{IReactorProcess.spawnProcess}.
        """
        self._workers.append(worker)
        worker.makeConnection(FakeTransport())
        self.spawnCount += 1


    def stop(self):
        """
        See L{IReactorCore.stop}.
        """
        MemoryReactorClock.stop(self)
        self.stopCount += 1


    def run(self):
        """
        See L{IReactorCore.run}.
        """
        self.runCount += 1

        # The same as IReactorCore.run, except no stop.
        self.running = True
        self.hasRun = True

        for f, args, kwargs in self.whenRunningHooks:
            f(*args, **kwargs)



class CountingReactorTests(SynchronousTestCase):
    """
    Tests for L{CountingReactor}.
    """

    def setUp(self):
        self.workers = []
        self.reactor = CountingReactor(self.workers)


    def test_providesIReactorProcess(self):
        """
        L{CountingReactor} instances provide L{IReactorProcess}.
        """
        verify.verifyObject(interfaces.IReactorProcess, self.reactor)


    def test_spawnProcess(self):
        """
        The process protocol for a spawned process is connected to a
        transport and appended onto the provided C{workers} list, and
        the reactor's C{spawnCount} increased.
        """
        self.assertFalse(self.reactor.spawnCount)

        proto = Protocol()
        for count in [1, 2]:
            self.reactor.spawnProcess(proto, sys.executable,
                                      arg=[sys.executable])
            self.assertTrue(proto.transport)
            self.assertEqual(self.workers, [proto] * count)
            self.assertEqual(self.reactor.spawnCount, count)


    def test_stop(self):
        """
        Stopping the reactor increments its C{stopCount}
        """
        self.assertFalse(self.reactor.stopCount)
        for count in [1, 2]:
            self.reactor.stop()
            self.assertEqual(self.reactor.stopCount, count)


    def test_run(self):
        """
        Running the reactor increments its C{runCount}, does not imply
        C{stop}, and calls L{IReactorCore.callWhenRunning} hooks.
        """
        self.assertFalse(self.reactor.runCount)

        whenRunningCalls = []
        self.reactor.callWhenRunning(whenRunningCalls.append, None)

        for count in [1, 2]:
            self.reactor.run()
            self.assertEqual(self.reactor.runCount, count)
            self.assertEqual(self.reactor.stopCount, 0)
            self.assertEqual(len(whenRunningCalls), count)



class EternalTerminationPredicateFactory(object):
    """
    A rigged terminationPredicateFactory for which time never pass.
    """

    def __call__(self):
        """
        See: L{task._Timer}
        """
        return False



class DistTrialRunnerTests(TestCase):
    """
    Tests for L{DistTrialRunner}.
    """

    def setUp(self):
        """
        Create a runner for testing.
        """
        self.runner = DistTrialRunner(TreeReporter, 4, [],
                                      workingDirectory=self.mktemp())
        self.runner._stream = StringIO()


    def reap(self, workers):
        """
        Reap the workers and trap L{ConnectionDone} failures on their
        C{endDeferred}s.

        @param workers: The workers to reap.
        @type workers: An iterable of L{LocalWorker}
        """

        for worker in workers:
            worker.endDeferred.addErrback(Failure.trap, error.ConnectionDone)
            worker.processEnded(Failure(CONNECTION_DONE))


    def getFakeSchedulerAndEternalCooperator(self):
        """
        Helper to create fake scheduler and cooperator in tests.

        The cooperator has a termination timer which will never inform
        the scheduler that the task needs to be terminated.

        @return: L{tuple} of (scheduler, cooperator)
        """
        scheduler = FakeScheduler()
        cooperator = Cooperator(
            scheduler=scheduler,
            terminationPredicateFactory=EternalTerminationPredicateFactory,
            )
        return scheduler, cooperator


    def test_writeResults(self):
        """
        L{DistTrialRunner.writeResults} writes to the stream specified in the
        init.
        """
        stringIO = StringIO()
        result = DistReporter(Reporter(stringIO))
        self.runner.writeResults(result)
        self.assertTrue(stringIO.tell() > 0)


    def test_createLocalWorkers(self):
        """
        C{createLocalWorkers} iterates the list of protocols and create one
        L{LocalWorker} for each.
        """
        protocols = [object() for x in range(4)]
        workers = self.runner.createLocalWorkers(protocols, "path")
        for s in workers:
            self.assertIsInstance(s, LocalWorker)
        self.assertEqual(4, len(workers))


    def test_launchWorkerProcesses(self):
        """
        Given a C{spawnProcess} function, C{launchWorkerProcess} launches a
        python process with an existing path as its argument.
        """
        protocols = [ProcessProtocol() for i in range(4)]
        arguments = []
        environment = {}

        def fakeSpawnProcess(processProtocol, executable, args=(), env={},
                             path=None, uid=None, gid=None, usePTY=0,
                             childFDs=None):
            arguments.append(executable)
            arguments.extend(args)
            environment.update(env)

        self.runner.launchWorkerProcesses(
            fakeSpawnProcess, protocols, ["foo"])
        self.assertEqual(arguments[0], arguments[1])
        self.assertTrue(os.path.exists(arguments[2]))
        self.assertEqual("foo", arguments[3])
        self.assertEqual(os.pathsep.join(sys.path),
                         environment["TRIAL_PYTHONPATH"])


    def test_run(self):
        """
        C{run} starts the reactor exactly once and spawns each of the workers
        exactly once.
        """
        workers = []
        fakeReactor = CountingReactor(workers)
        self.addCleanup(self.reap, workers)

        suite = TrialSuite()
        for i in range(10):
            suite.addTest(TestCase())
        self.runner.run(suite, fakeReactor)
        self.assertEqual(fakeReactor.runCount, 1)
        self.assertEqual(fakeReactor.spawnCount, self.runner._workerNumber)


    def test_runUsedDirectory(self):
        """
        L{DistTrialRunner} checks if the test directory is already locked, and
        if it is generates a name based on it.
        """

        class CountingReactorWithLock(CountingReactor):

            def spawnProcess(oself, worker, *args, **kwargs):
                oself._workers.append(worker)
                self.assertEqual(os.path.abspath(worker._logDirectory),
                                 os.path.abspath(
                                     os.path.join(workingDirectory + "-1",
                                                  str(oself.spawnCount))))
                localLock = FilesystemLock(workingDirectory + "-1.lock")
                self.assertFalse(localLock.lock())
                oself.spawnCount += 1
                worker.makeConnection(FakeTransport())
                worker._ampProtocol.run = lambda *args: succeed(None)

        newDirectory = self.mktemp()
        os.mkdir(newDirectory)
        workingDirectory = os.path.join(newDirectory, "_trial_temp")
        lock = FilesystemLock(workingDirectory + ".lock")
        lock.lock()
        self.addCleanup(lock.unlock)
        self.runner._workingDirectory = workingDirectory

        workers = []

        fakeReactor = CountingReactorWithLock(workers)
        self.addCleanup(self.reap, workers)

        suite = TrialSuite()
        for i in range(10):
            suite.addTest(TestCase())

        self.runner.run(suite, fakeReactor)


    def test_minimalWorker(self):
        """
        L{DistTrialRunner} doesn't try to start more workers than the number of
        tests.
        """
        workers = []
        fakeReactor = CountingReactor(workers)
        self.addCleanup(self.reap, workers)

        self.runner.run(TestCase(), fakeReactor)
        self.assertEqual(fakeReactor.runCount, 1)
        self.assertEqual(fakeReactor.spawnCount, 1)


    def test_runUncleanWarnings(self):
        """
        Running with the C{unclean-warnings} option makes L{DistTrialRunner}
        uses the L{UncleanWarningsReporterWrapper}.
        """
        workers = []
        fakeReactor = CountingReactor(workers)
        self.addCleanup(self.reap, workers)

        self.runner._uncleanWarnings = True
        result = self.runner.run(TestCase(), fakeReactor)
        self.assertIsInstance(result, DistReporter)
        self.assertIsInstance(result.original,
                              UncleanWarningsReporterWrapper)


    def test_runWithoutTest(self):
        """
        When the suite contains no test, L{DistTrialRunner} takes a shortcut
        path without launching any process or starting the reactor.
        """
        fakeReactor = object()
        suite = TrialSuite()
        result = self.runner.run(suite, fakeReactor)
        self.assertIsInstance(result, DistReporter)
        output = self.runner._stream.getvalue()
        self.assertIn("Running 0 test", output)
        self.assertIn("PASSED", output)


    def test_runWithoutTestButWithAnError(self):
        """
        Even if there is no test, the suite can contain an error (most likely,
        an import error): this should make the run fail, and the error should
        be printed.
        """
        fakeReactor = object()
        error = ErrorHolder("an error", Failure(RuntimeError("foo bar")))
        result = self.runner.run(error, fakeReactor)
        self.assertIsInstance(result, DistReporter)
        output = self.runner._stream.getvalue()
        self.assertIn("Running 0 test", output)
        self.assertIn("foo bar", output)
        self.assertIn("an error", output)
        self.assertIn("errors=1", output)
        self.assertIn("FAILED", output)


    def test_runUnexpectedError(self):
        """
        If for some reasons we can't connect to the worker process, the test
        suite catches and fails.
        """

        class CountingReactorWithFail(CountingReactor):

            def spawnProcess(self, worker, *args, **kwargs):
                self._workers.append(worker)
                worker.makeConnection(FakeTransport())
                self.spawnCount += 1
                worker._ampProtocol.run = self.failingRun

            def failingRun(self, case, result):
                return fail(RuntimeError("oops"))

        scheduler, cooperator = self.getFakeSchedulerAndEternalCooperator()

        workers = []
        fakeReactor = CountingReactorWithFail(workers)
        self.addCleanup(self.reap, workers)

        result = self.runner.run(TestCase(), fakeReactor,
                                 cooperator.cooperate)
        self.assertEqual(fakeReactor.runCount, 1)
        self.assertEqual(fakeReactor.spawnCount, 1)
        scheduler.pump()
        self.assertEqual(1, len(result.original.failures))


    def test_runStopAfterTests(self):
        """
        L{DistTrialRunner} calls C{reactor.stop} and unlocks the test directory
        once the tests have run.
        """
        class CountingReactorWithSuccess(CountingReactor):

            def spawnProcess(self, worker, *args, **kwargs):
                self._workers.append(worker)
                worker.makeConnection(FakeTransport())
                self.spawnCount += 1
                worker._ampProtocol.run = self.succeedingRun

            def succeedingRun(self, case, result):
                return succeed(None)

        workingDirectory = self.runner._workingDirectory

        workers = []
        fakeReactor = CountingReactorWithSuccess(workers)

        self.runner.run(TestCase(), fakeReactor)

        def check():
            localLock = FilesystemLock(workingDirectory + ".lock")
            self.assertTrue(localLock.lock())
            self.assertEqual(1, fakeReactor.stopCount)

        self.assertEqual(list(fakeReactor.triggers.keys()), ["before"])
        self.assertEqual(list(fakeReactor.triggers["before"]), ["shutdown"])
        self.reap(workers)

        return deferLater(reactor, 0, check)


    def test_runWaitForProcessesDeferreds(self):
        """
        L{DistTrialRunner} waits for the worker processes to stop when the
        reactor is stopping, and then unlocks the test directory, not trying to
        stop the reactor again.
        """
        workers = []
        workingDirectory = self.runner._workingDirectory

        fakeReactor = CountingReactor(workers)
        self.runner.run(TestCase(), fakeReactor)

        def check(ign):
            # Let the AMP deferreds fire
            return deferLater(reactor, 0, realCheck)

        def realCheck():
            localLock = FilesystemLock(workingDirectory + ".lock")
            self.assertTrue(localLock.lock())
            # Stop is not called, as it ought to have been called before
            self.assertEqual(0, fakeReactor.stopCount)

        self.assertEqual(list(fakeReactor.triggers.keys()), ["before"])
        self.assertEqual(list(fakeReactor.triggers["before"]), ["shutdown"])
        self.reap(workers)

        return gatherResults([
            maybeDeferred(f, *a, **kw)
            for f, a, kw in fakeReactor.triggers["before"]["shutdown"]
        ]).addCallback(check)


    def test_runUntilFailure(self):
        """
        L{DistTrialRunner} can run in C{untilFailure} mode where it will run
        the given tests until they fail.
        """
        called = []

        class CountingReactorWithSuccess(CountingReactor):

            def spawnProcess(self, worker, *args, **kwargs):
                self._workers.append(worker)
                worker.makeConnection(FakeTransport())
                self.spawnCount += 1
                worker._ampProtocol.run = self.succeedingRun

            def succeedingRun(self, case, result):
                called.append(None)
                if len(called) == 5:
                    return fail(RuntimeError("oops"))
                return succeed(None)

        workers = []
        fakeReactor = CountingReactorWithSuccess(workers)
        self.addCleanup(self.reap, workers)

        scheduler, cooperator = self.getFakeSchedulerAndEternalCooperator()

        result = self.runner.run(
            TestCase(), fakeReactor, cooperate=cooperator.cooperate,
            untilFailure=True)
        scheduler.pump()
        self.assertEqual(5, len(called))
        self.assertFalse(result.wasSuccessful())
        output = self.runner._stream.getvalue()
        self.assertIn("PASSED", output)
        self.assertIn("FAIL", output)
