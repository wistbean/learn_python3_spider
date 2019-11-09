# -*- test-case-name: twisted.application.runner.test.test_runner -*-
# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Twisted application runner.
"""

from sys import stderr
from signal import SIGTERM
from os import kill

from attr import attrib, attrs, Factory

from twisted.logger import (
    globalLogBeginner, textFileLogObserver,
    FilteringLogObserver, LogLevelFilterPredicate,
    LogLevel, Logger,
)

from ._exit import exit, ExitStatus
from ._pidfile import nonePIDFile, AlreadyRunningError, InvalidPIDFileError



@attrs(frozen=True)
class Runner(object):
    """
    Twisted application runner.

    @cvar _log: The logger attached to this class.
    @type _log: L{Logger}

    @ivar _reactor: The reactor to start and run the application in.
    @type _reactor: L{IReactorCore}

    @ivar _pidFile: The file to store the running process ID in.
    @type _pidFile: L{IPIDFile}

    @ivar _kill: Whether this runner should kill an existing running
        instance of the application.
    @type _kill: L{bool}

    @ivar _defaultLogLevel: The default log level to start the logging
        system with.
    @type _defaultLogLevel: L{constantly.NamedConstant} from L{LogLevel}

    @ivar _logFile: A file stream to write logging output to.
    @type _logFile: writable file-like object

    @ivar _fileLogObserverFactory: A factory for the file log observer to
        use when starting the logging system.
    @type _pidFile: callable that takes a single writable file-like object
        argument and returns a L{twisted.logger.FileLogObserver}

    @ivar _whenRunning: Hook to call after the reactor is running;
        this is where the application code that relies on the reactor gets
        called.
    @type _whenRunning: callable that takes the keyword arguments specified
        by C{whenRunningArguments}

    @ivar _whenRunningArguments: Keyword arguments to pass to
        C{whenRunning} when it is called.
    @type _whenRunningArguments: L{dict}

    @ivar _reactorExited: Hook to call after the reactor exits.
    @type _reactorExited: callable that takes the keyword arguments
        specified by C{reactorExitedArguments}

    @ivar _reactorExitedArguments: Keyword arguments to pass to
        C{reactorExited} when it is called.
    @type _reactorExitedArguments: L{dict}
    """

    _log = Logger()

    _reactor                = attrib()
    _pidFile                = attrib(default=nonePIDFile)
    _kill                   = attrib(default=False)
    _defaultLogLevel        = attrib(default=LogLevel.info)
    _logFile                = attrib(default=stderr)
    _fileLogObserverFactory = attrib(default=textFileLogObserver)
    _whenRunning            = attrib(default=lambda **_: None)
    _whenRunningArguments   = attrib(default=Factory(dict))
    _reactorExited          = attrib(default=lambda **_: None)
    _reactorExitedArguments = attrib(default=Factory(dict))


    def run(self):
        """
        Run this command.
        """
        pidFile = self._pidFile

        self.killIfRequested()

        try:
            with pidFile:
                self.startLogging()
                self.startReactor()
                self.reactorExited()

        except AlreadyRunningError:
            exit(ExitStatus.EX_CONFIG, "Already running.")
            return  # When testing, patched exit doesn't exit


    def killIfRequested(self):
        """
        If C{self._kill} is true, attempt to kill a running instance of the
        application.
        """
        pidFile = self._pidFile

        if self._kill:
            if pidFile is nonePIDFile:
                exit(ExitStatus.EX_USAGE, "No PID file specified.")
                return  # When testing, patched exit doesn't exit

            try:
                pid = pidFile.read()
            except EnvironmentError:
                exit(ExitStatus.EX_IOERR, "Unable to read PID file.")
                return  # When testing, patched exit doesn't exit
            except InvalidPIDFileError:
                exit(ExitStatus.EX_DATAERR, "Invalid PID file.")
                return  # When testing, patched exit doesn't exit

            self.startLogging()
            self._log.info("Terminating process: {pid}", pid=pid)

            kill(pid, SIGTERM)

            exit(ExitStatus.EX_OK)
            return  # When testing, patched exit doesn't exit


    def startLogging(self):
        """
        Start the L{twisted.logger} logging system.
        """
        logFile = self._logFile

        fileLogObserverFactory = self._fileLogObserverFactory

        fileLogObserver = fileLogObserverFactory(logFile)

        logLevelPredicate = LogLevelFilterPredicate(
            defaultLogLevel=self._defaultLogLevel
        )

        filteringObserver = FilteringLogObserver(
            fileLogObserver, [logLevelPredicate]
        )

        globalLogBeginner.beginLoggingTo([filteringObserver])


    def startReactor(self):
        """
        Register C{self._whenRunning} with the reactor so that it is called
        once the reactor is running, then start the reactor.
        """
        self._reactor.callWhenRunning(self.whenRunning)

        self._log.info("Starting reactor...")
        self._reactor.run()


    def whenRunning(self):
        """
        Call C{self._whenRunning} with C{self._whenRunningArguments}.

        @note: This method is called after the reactor starts running.
        """
        self._whenRunning(**self._whenRunningArguments)


    def reactorExited(self):
        """
        Call C{self._reactorExited} with C{self._reactorExitedArguments}.

        @note: This method is called after the reactor exits.
        """
        self._reactorExited(**self._reactorExitedArguments)
