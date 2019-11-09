# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Tests for L{twisted.application.runner._pidfile}.
"""

from functools import wraps
import errno
from os import getpid, name as SYSTEM_NAME
from io import BytesIO

from zope.interface import implementer
from zope.interface.verify import verifyObject

from twisted.python.filepath import IFilePath
from twisted.python.runtime import platform

from ...runner import _pidfile
from .._pidfile import (
    IPIDFile, PIDFile, NonePIDFile,
    AlreadyRunningError, InvalidPIDFileError, StalePIDFileError,
    NoPIDFound,
)

import twisted.trial.unittest
from twisted.trial.unittest import SkipTest


def ifPlatformSupported(f):
    """
    Decorator for tests that are not expected to work on all platforms.

    Calling L{PIDFile.isRunning} currently raises L{NotImplementedError} on
    non-POSIX platforms.

    On an unsupported platform, we expect to see any test that calls
    L{PIDFile.isRunning} to raise either L{NotImplementedError}, L{SkipTest},
    or C{self.failureException}.
    (C{self.failureException} may occur in a test that checks for a specific
    exception but it gets NotImplementedError instead.)

    @param f: The test method to decorate.
    @type f: method

    @return: The wrapped callable.
    """
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        supported = platform.getType() == "posix"

        if supported:
            return f(self, *args, **kwargs)
        else:
            e = self.assertRaises(
                (NotImplementedError, SkipTest, self.failureException),
                f, self, *args, **kwargs
            )
            if isinstance(e, NotImplementedError):
                self.assertTrue(
                    str(e).startswith("isRunning is not implemented on ")
                )

    return wrapper



class PIDFileTests(twisted.trial.unittest.TestCase):
    """
    Tests for L{PIDFile}.
    """

    def test_interface(self):
        """
        L{PIDFile} conforms to L{IPIDFile}.
        """
        pidFile = PIDFile(DummyFilePath())
        verifyObject(IPIDFile, pidFile)


    def test_formatWithPID(self):
        """
        L{PIDFile._format} returns the expected format when given a PID.
        """
        self.assertEqual(PIDFile._format(pid=1337), b"1337\n")


    def test_readWithPID(self):
        """
        L{PIDFile.read} returns the PID from the given file path.
        """
        pid = 1337

        pidFile = PIDFile(DummyFilePath(PIDFile._format(pid=pid)))

        self.assertEqual(pid, pidFile.read())


    def test_readEmptyPID(self):
        """
        L{PIDFile.read} raises L{InvalidPIDFileError} when given an empty file
        path.
        """
        pidValue = b""
        pidFile = PIDFile(DummyFilePath(b""))

        e = self.assertRaises(InvalidPIDFileError, pidFile.read)
        self.assertEqual(
            str(e),
            "non-integer PID value in PID file: {!r}".format(pidValue)
        )


    def test_readWithBogusPID(self):
        """
        L{PIDFile.read} raises L{InvalidPIDFileError} when given an empty file
        path.
        """
        pidValue = b"$foo!"
        pidFile = PIDFile(DummyFilePath(pidValue))

        e = self.assertRaises(InvalidPIDFileError, pidFile.read)
        self.assertEqual(
            str(e),
            "non-integer PID value in PID file: {!r}".format(pidValue)
        )


    def test_readDoesntExist(self):
        """
        L{PIDFile.read} raises L{NoPIDFound} when given a non-existing file
        path.
        """
        pidFile = PIDFile(DummyFilePath())

        e = self.assertRaises(NoPIDFound, pidFile.read)
        self.assertEqual(str(e), "PID file does not exist")


    def test_readOpenRaisesOSErrorNotENOENT(self):
        """
        L{PIDFile.read} re-raises L{OSError} if the associated C{errno} is
        anything other than L{errno.ENOENT}.
        """
        def oops(mode="r"):
            raise OSError(errno.EIO, "I/O error")

        self.patch(DummyFilePath, "open", oops)

        pidFile = PIDFile(DummyFilePath())

        error = self.assertRaises(OSError, pidFile.read)
        self.assertEqual(error.errno, errno.EIO)


    def test_writePID(self):
        """
        L{PIDFile._write} stores the given PID.
        """
        pid = 1995

        pidFile = PIDFile(DummyFilePath())
        pidFile._write(pid)

        self.assertEqual(pidFile.read(), pid)


    def test_writePIDInvalid(self):
        """
        L{PIDFile._write} raises L{ValueError} when given an invalid PID.
        """
        pidFile = PIDFile(DummyFilePath())

        self.assertRaises(ValueError, pidFile._write, u"burp")


    def test_writeRunningPID(self):
        """
        L{PIDFile.writeRunningPID} stores the PID for the current process.
        """
        pidFile = PIDFile(DummyFilePath())
        pidFile.writeRunningPID()

        self.assertEqual(pidFile.read(), getpid())


    def test_remove(self):
        """
        L{PIDFile.remove} removes the PID file.
        """
        pidFile = PIDFile(DummyFilePath(b""))
        self.assertTrue(pidFile.filePath.exists())

        pidFile.remove()
        self.assertFalse(pidFile.filePath.exists())


    @ifPlatformSupported
    def test_isRunningDoesExist(self):
        """
        L{PIDFile.isRunning} returns true for a process that does exist.
        """
        pidFile = PIDFile(DummyFilePath())
        pidFile._write(1337)

        def kill(pid, signal):
            return  # Don't actually kill anything

        self.patch(_pidfile, "kill", kill)

        self.assertTrue(pidFile.isRunning())


    @ifPlatformSupported
    def test_isRunningThis(self):
        """
        L{PIDFile.isRunning} returns true for this process (which is running).

        @note: This differs from L{PIDFileTests.test_isRunningDoesExist} in
        that it actually invokes the C{kill} system call, which is useful for
        testing of our chosen method for probing the existence of a process.
        """
        pidFile = PIDFile(DummyFilePath())
        pidFile.writeRunningPID()

        self.assertTrue(pidFile.isRunning())


    @ifPlatformSupported
    def test_isRunningDoesNotExist(self):
        """
        L{PIDFile.isRunning} raises L{StalePIDFileError} for a process that
        does not exist (errno=ESRCH).
        """
        pidFile = PIDFile(DummyFilePath())
        pidFile._write(1337)

        def kill(pid, signal):
            raise OSError(errno.ESRCH, "No such process")

        self.patch(_pidfile, "kill", kill)

        self.assertRaises(StalePIDFileError, pidFile.isRunning)


    @ifPlatformSupported
    def test_isRunningNotAllowed(self):
        """
        L{PIDFile.isRunning} returns true for a process that we are not allowed
        to kill (errno=EPERM).
        """
        pidFile = PIDFile(DummyFilePath())
        pidFile._write(1337)

        def kill(pid, signal):
            raise OSError(errno.EPERM, "Operation not permitted")

        self.patch(_pidfile, "kill", kill)

        self.assertTrue(pidFile.isRunning())


    @ifPlatformSupported
    def test_isRunningInit(self):
        """
        L{PIDFile.isRunning} returns true for a process that we are not allowed
        to kill (errno=EPERM).

        @note: This differs from L{PIDFileTests.test_isRunningNotAllowed} in
        that it actually invokes the C{kill} system call, which is useful for
        testing of our chosen method for probing the existence of a process
        that we are not allowed to kill.

        @note: In this case, we try killing C{init}, which is process #1 on
        POSIX systems, so this test is not portable.  C{init} should always be
        running and should not be killable by non-root users.
        """
        if SYSTEM_NAME != "posix":
            raise SkipTest("This test assumes POSIX")

        pidFile = PIDFile(DummyFilePath())
        pidFile._write(1)  # PID 1 is init on POSIX systems

        self.assertTrue(pidFile.isRunning())


    @ifPlatformSupported
    def test_isRunningUnknownErrno(self):
        """
        L{PIDFile.isRunning} re-raises L{OSError} if the attached C{errno}
        value from L{os.kill} is not an expected one.
        """
        pidFile = PIDFile(DummyFilePath())
        pidFile.writeRunningPID()

        def kill(pid, signal):
            raise OSError(errno.EEXIST, "File exists")

        self.patch(_pidfile, "kill", kill)

        self.assertRaises(OSError, pidFile.isRunning)


    def test_isRunningNoPIDFile(self):
        """
        L{PIDFile.isRunning} returns false if the PID file doesn't exist.
        """
        pidFile = PIDFile(DummyFilePath())

        self.assertFalse(pidFile.isRunning())


    def test_contextManager(self):
        """
        When used as a context manager, a L{PIDFile} will store the current pid
        on entry, then removes the PID file on exit.
        """
        pidFile = PIDFile(DummyFilePath())
        self.assertFalse(pidFile.filePath.exists())

        with pidFile:
            self.assertTrue(pidFile.filePath.exists())
            self.assertEqual(pidFile.read(), getpid())

        self.assertFalse(pidFile.filePath.exists())


    @ifPlatformSupported
    def test_contextManagerDoesntExist(self):
        """
        When used as a context manager, a L{PIDFile} will replace the
        underlying PIDFile rather than raising L{AlreadyRunningError} if the
        contained PID file exists but refers to a non-running PID.
        """
        pidFile = PIDFile(DummyFilePath())
        pidFile._write(1337)

        def kill(pid, signal):
            raise OSError(errno.ESRCH, "No such process")

        self.patch(_pidfile, "kill", kill)

        e = self.assertRaises(StalePIDFileError, pidFile.isRunning)
        self.assertEqual(str(e), "PID file refers to non-existing process")

        with pidFile:
            self.assertEqual(pidFile.read(), getpid())


    @ifPlatformSupported
    def test_contextManagerAlreadyRunning(self):
        """
        When used as a context manager, a L{PIDFile} will raise
        L{AlreadyRunningError} if the there is already a running process with
        the contained PID.
        """
        pidFile = PIDFile(DummyFilePath())
        pidFile._write(1337)

        def kill(pid, signal):
            return  # Don't actually kill anything

        self.patch(_pidfile, "kill", kill)

        self.assertTrue(pidFile.isRunning())

        self.assertRaises(AlreadyRunningError, pidFile.__enter__)



class NonePIDFileTests(twisted.trial.unittest.TestCase):
    """
    Tests for L{NonePIDFile}.
    """

    def test_interface(self):
        """
        L{NonePIDFile} conforms to L{IPIDFile}.
        """
        pidFile = NonePIDFile()
        verifyObject(IPIDFile, pidFile)


    def test_read(self):
        """
        L{NonePIDFile.read} raises L{NoPIDFound}.
        """
        pidFile = NonePIDFile()

        e = self.assertRaises(NoPIDFound, pidFile.read)
        self.assertEqual(str(e), "PID file does not exist")


    def test_write(self):
        """
        L{NonePIDFile._write} raises L{OSError} with an errno of L{errno.EPERM}.
        """
        pidFile = NonePIDFile()

        error = self.assertRaises(OSError, pidFile._write, 0)
        self.assertEqual(error.errno, errno.EPERM)


    def test_writeRunningPID(self):
        """
        L{NonePIDFile.writeRunningPID} raises L{OSError} with an errno of
        L{errno.EPERM}.
        """
        pidFile = NonePIDFile()

        error = self.assertRaises(OSError, pidFile.writeRunningPID)
        self.assertEqual(error.errno, errno.EPERM)


    def test_remove(self):
        """
        L{NonePIDFile.remove} raises L{OSError} with an errno of L{errno.EPERM}.
        """
        pidFile = NonePIDFile()

        error = self.assertRaises(OSError, pidFile.remove)
        self.assertEqual(error.errno, errno.ENOENT)


    def test_isRunning(self):
        """
        L{NonePIDFile.isRunning} returns L{False}.
        """
        pidFile = NonePIDFile()

        self.assertEqual(pidFile.isRunning(), False)


    def test_contextManager(self):
        """
        When used as a context manager, a L{NonePIDFile} doesn't raise, despite
        not existing.
        """
        pidFile = NonePIDFile()

        with pidFile:
            pass



@implementer(IFilePath)
class DummyFilePath(object):
    """
    In-memory L{IFilePath}.
    """

    def __init__(self, content=None):
        self.setContent(content)


    def open(self, mode="r"):
        if not self._exists:
            raise OSError(errno.ENOENT, "No such file or directory")
        return BytesIO(self.getContent())


    def setContent(self, content):
        self._exists = content is not None
        self._content = content


    def getContent(self):
        return self._content


    def remove(self):
        self.setContent(None)


    def exists(self):
        return self._exists
