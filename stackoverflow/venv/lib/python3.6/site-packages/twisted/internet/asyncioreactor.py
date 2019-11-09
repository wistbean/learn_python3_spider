# -*- test-case-name: twisted.test.test_internet -*-
# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
asyncio-based reactor implementation.
"""

from __future__ import absolute_import, division

import errno

from zope.interface import implementer

from twisted.logger import Logger
from twisted.internet.base import DelayedCall
from twisted.internet.posixbase import (PosixReactorBase, _NO_FILEDESC,
                                        _ContinuousPolling)
from twisted.python.log import callWithLogger
from twisted.internet.interfaces import IReactorFDSet

try:
    from asyncio import get_event_loop
except ImportError:
    raise ImportError("Requires asyncio.")

# As per ImportError above, this module is never imported on python 2, but
# pyflakes still runs on python 2, so let's tell it where the errors come from.
from builtins import PermissionError, BrokenPipeError


class _DCHandle(object):
    """
    Wraps ephemeral L{asyncio.Handle} instances.  Callbacks can close
    over this and use it as a mutable reference to asyncio C{Handles}.

    @ivar handle: The current L{asyncio.Handle}
    """
    def __init__(self, handle):
        self.handle = handle


    def cancel(self):
        """
        Cancel the inner L{asyncio.Handle}.
        """
        self.handle.cancel()



@implementer(IReactorFDSet)
class AsyncioSelectorReactor(PosixReactorBase):
    """
    Reactor running on top of L{asyncio.SelectorEventLoop}.
    """
    _asyncClosed = False
    _log = Logger()

    def __init__(self, eventloop=None):

        if eventloop is None:
            eventloop = get_event_loop()

        self._asyncioEventloop = eventloop
        self._writers = {}
        self._readers = {}
        self._delayedCalls = set()
        self._continuousPolling = _ContinuousPolling(self)
        super().__init__()


    def _unregisterFDInAsyncio(self, fd):
        """
        Compensate for a bug in asyncio where it will not unregister a FD that
        it cannot handle in the epoll loop. It touches internal asyncio code.

        A description of the bug by markrwilliams:

        The C{add_writer} method of asyncio event loops isn't atomic because
        all the Selector classes in the selector module internally record a
        file object before passing it to the platform's selector
        implementation. If the platform's selector decides the file object
        isn't acceptable, the resulting exception doesn't cause the Selector to
        un-track the file object.

        The failing/hanging stdio test goes through the following sequence of
        events (roughly):

        * The first C{connection.write(intToByte(value))} call hits the asyncio
        reactor's C{addWriter} method.

        * C{addWriter} calls the asyncio loop's C{add_writer} method, which
        happens to live on C{_BaseSelectorEventLoop}.

        * The asyncio loop's C{add_writer} method checks if the file object has
        been registered before via the selector's C{get_key} method.

        * It hasn't, so the KeyError block runs and calls the selector's
        register method

        * Code examples that follow use EpollSelector, but the code flow holds
        true for any other selector implementation. The selector's register
        method first calls through to the next register method in the MRO

        * That next method is always C{_BaseSelectorImpl.register} which
        creates a C{SelectorKey} instance for the file object, stores it under
        the file object's file descriptor, and then returns it.

        * Control returns to the concrete selector implementation, which asks
        the operating system to track the file descriptor using the right API.

        * The operating system refuses! An exception is raised that, in this
        case, the asyncio reactor handles by creating a C{_ContinuousPolling}
        object to watch the file descriptor.

        * The second C{connection.write(intToByte(value))} call hits the
        asyncio reactor's C{addWriter} method, which hits the C{add_writer}
        method. But the loop's selector's get_key method now returns a
        C{SelectorKey}! Now the asyncio reactor's C{addWriter} method thinks
        the asyncio loop will watch the file descriptor, even though it won't.
        """
        try:
            self._asyncioEventloop._selector.unregister(fd)
        except:
            pass


    def _readOrWrite(self, selectable, read):
        method = selectable.doRead if read else selectable.doWrite

        if selectable.fileno() == -1:
            self._disconnectSelectable(selectable, _NO_FILEDESC, read)
            return

        try:
            why = method()
        except Exception as e:
            why = e
            self._log.failure(None)
        if why:
            self._disconnectSelectable(selectable, why, read)


    def addReader(self, reader):
        if reader in self._readers.keys() or \
           reader in self._continuousPolling._readers:
            return

        fd = reader.fileno()
        try:
            self._asyncioEventloop.add_reader(fd, callWithLogger, reader,
                                              self._readOrWrite, reader,
                                              True)
            self._readers[reader] = fd
        except IOError as e:
            self._unregisterFDInAsyncio(fd)
            if e.errno == errno.EPERM:
                # epoll(7) doesn't support certain file descriptors,
                # e.g. filesystem files, so for those we just poll
                # continuously:
                self._continuousPolling.addReader(reader)
            else:
                raise


    def addWriter(self, writer):
        if writer in self._writers.keys() or \
           writer in self._continuousPolling._writers:
            return

        fd = writer.fileno()
        try:
            self._asyncioEventloop.add_writer(fd, callWithLogger, writer,
                                              self._readOrWrite, writer,
                                              False)
            self._writers[writer] = fd
        except PermissionError:
            self._unregisterFDInAsyncio(fd)
            # epoll(7) doesn't support certain file descriptors,
            # e.g. filesystem files, so for those we just poll
            # continuously:
            self._continuousPolling.addWriter(writer)
        except BrokenPipeError:
            # The kqueuereactor will raise this if there is a broken pipe
            self._unregisterFDInAsyncio(fd)
        except:
            self._unregisterFDInAsyncio(fd)
            raise


    def removeReader(self, reader):

        # First, see if they're trying to remove a reader that we don't have.
        if not (reader in self._readers.keys() \
                or self._continuousPolling.isReading(reader)):
            # We don't have it, so just return OK.
            return

        # If it was a cont. polling reader, check there first.
        if self._continuousPolling.isReading(reader):
            self._continuousPolling.removeReader(reader)
            return

        fd = reader.fileno()
        if fd == -1:
            # If the FD is -1, we want to know what its original FD was, to
            # remove it.
            fd = self._readers.pop(reader)
        else:
            self._readers.pop(reader)

        self._asyncioEventloop.remove_reader(fd)


    def removeWriter(self, writer):

        # First, see if they're trying to remove a writer that we don't have.
        if not (writer in self._writers.keys() \
                or self._continuousPolling.isWriting(writer)):
            # We don't have it, so just return OK.
            return

        # If it was a cont. polling writer, check there first.
        if self._continuousPolling.isWriting(writer):
            self._continuousPolling.removeWriter(writer)
            return

        fd = writer.fileno()

        if fd == -1:
            # If the FD is -1, we want to know what its original FD was, to
            # remove it.
            fd = self._writers.pop(writer)
        else:
            self._writers.pop(writer)

        self._asyncioEventloop.remove_writer(fd)


    def removeAll(self):
        return (self._removeAll(self._readers.keys(), self._writers.keys()) +
                self._continuousPolling.removeAll())


    def getReaders(self):
        return (list(self._readers.keys()) +
                self._continuousPolling.getReaders())


    def getWriters(self):
        return (list(self._writers.keys()) +
                self._continuousPolling.getWriters())


    def getDelayedCalls(self):
        return list(self._delayedCalls)


    def iterate(self, timeout):
        self._asyncioEventloop.call_later(timeout + 0.01,
                                          self._asyncioEventloop.stop)
        self._asyncioEventloop.run_forever()


    def run(self, installSignalHandlers=True):
        self.startRunning(installSignalHandlers=installSignalHandlers)
        self._asyncioEventloop.run_forever()
        if self._justStopped:
            self._justStopped = False


    def stop(self):
        super().stop()
        self.callLater(0, self.fireSystemEvent, "shutdown")


    def crash(self):
        super().crash()
        self._asyncioEventloop.stop()


    def seconds(self):
        return self._asyncioEventloop.time()


    def callLater(self, seconds, f, *args, **kwargs):
        def run():
            dc.called = True
            self._delayedCalls.remove(dc)
            f(*args, **kwargs)
        handle = self._asyncioEventloop.call_later(seconds, run)
        dchandle = _DCHandle(handle)

        def cancel(dc):
            self._delayedCalls.remove(dc)
            dchandle.cancel()

        def reset(dc):
            dchandle.handle = self._asyncioEventloop.call_at(dc.time, run)

        dc = DelayedCall(self.seconds() + seconds, run, (), {},
                         cancel, reset, seconds=self.seconds)
        self._delayedCalls.add(dc)
        return dc


    def callFromThread(self, f, *args, **kwargs):
        g = lambda: self.callLater(0, f, *args, **kwargs)
        self._asyncioEventloop.call_soon_threadsafe(g)



def install(eventloop=None):
    """
    Install an asyncio-based reactor.

    @param eventloop: The asyncio eventloop to wrap. If default, the global one
        is selected.
    """
    reactor = AsyncioSelectorReactor(eventloop)
    from twisted.internet.main import installReactor
    installReactor(reactor)
