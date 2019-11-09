# Copyright 2012-2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Utilities for multi-threading support."""

import threading
try:
    from time import monotonic as _time
except ImportError:
    from time import time as _time

from pymongo.monotonic import time as _time
from pymongo.errors import ExceededMaxWaiters


### Begin backport from CPython 3.2 for timeout support for Semaphore.acquire
class Semaphore:

    # After Tim Peters' semaphore class, but not quite the same (no maximum)

    def __init__(self, value=1):
        if value < 0:
            raise ValueError("semaphore initial value must be >= 0")
        self._cond = threading.Condition(threading.Lock())
        self._value = value

    def acquire(self, blocking=True, timeout=None):
        if not blocking and timeout is not None:
            raise ValueError("can't specify timeout for non-blocking acquire")
        rc = False
        endtime = None
        self._cond.acquire()
        while self._value == 0:
            if not blocking:
                break
            if timeout is not None:
                if endtime is None:
                    endtime = _time() + timeout
                else:
                    timeout = endtime - _time()
                    if timeout <= 0:
                        break
            self._cond.wait(timeout)
        else:
            self._value = self._value - 1
            rc = True
        self._cond.release()
        return rc

    __enter__ = acquire

    def release(self):
        self._cond.acquire()
        self._value = self._value + 1
        self._cond.notify()
        self._cond.release()

    def __exit__(self, t, v, tb):
        self.release()

    @property
    def counter(self):
        return self._value


class BoundedSemaphore(Semaphore):
    """Semaphore that checks that # releases is <= # acquires"""
    def __init__(self, value=1):
        Semaphore.__init__(self, value)
        self._initial_value = value

    def release(self):
        if self._value >= self._initial_value:
            raise ValueError("Semaphore released too many times")
        return Semaphore.release(self)
### End backport from CPython 3.2


class DummySemaphore(object):
    def __init__(self, value=None):
        pass

    def acquire(self, blocking=True, timeout=None):
        return True

    def release(self):
        pass


class MaxWaitersBoundedSemaphore(object):
    def __init__(self, semaphore_class, value=1, max_waiters=1):
        self.waiter_semaphore = semaphore_class(max_waiters)
        self.semaphore = semaphore_class(value)

    def acquire(self, blocking=True, timeout=None):
        if not self.waiter_semaphore.acquire(False):
            raise ExceededMaxWaiters()
        try:
            return self.semaphore.acquire(blocking, timeout)
        finally:
            self.waiter_semaphore.release()

    def __getattr__(self, name):
        return getattr(self.semaphore, name)


class MaxWaitersBoundedSemaphoreThread(MaxWaitersBoundedSemaphore):
    def __init__(self, value=1, max_waiters=1):
        MaxWaitersBoundedSemaphore.__init__(
            self, BoundedSemaphore, value, max_waiters)


def create_semaphore(max_size, max_waiters):
    if max_size is None:
        return DummySemaphore()
    else:
        if max_waiters is None:
            return BoundedSemaphore(max_size)
        else:
            return MaxWaitersBoundedSemaphoreThread(max_size, max_waiters)
