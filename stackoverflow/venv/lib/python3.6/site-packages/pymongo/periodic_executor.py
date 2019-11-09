# Copyright 2014-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Run a target function on a background thread."""

import atexit
import threading
import time
import weakref

from pymongo.monotonic import time as _time


class PeriodicExecutor(object):
    def __init__(self, interval, min_interval, target, name=None):
        """"Run a target function periodically on a background thread.

        If the target's return value is false, the executor stops.

        :Parameters:
          - `interval`: Seconds between calls to `target`.
          - `min_interval`: Minimum seconds between calls if `wake` is
            called very often.
          - `target`: A function.
          - `name`: A name to give the underlying thread.
        """
        # threading.Event and its internal condition variable are expensive
        # in Python 2, see PYTHON-983. Use a boolean to know when to wake.
        # The executor's design is constrained by several Python issues, see
        # "periodic_executor.rst" in this repository.
        self._event = False
        self._interval = interval
        self._min_interval = min_interval
        self._target = target
        self._stopped = False
        self._thread = None
        self._name = name

        self._thread_will_exit = False
        self._lock = threading.Lock()

    def open(self):
        """Start. Multiple calls have no effect.

        Not safe to call from multiple threads at once.
        """
        with self._lock:
            if self._thread_will_exit:
                # If the background thread has read self._stopped as True
                # there is a chance that it has not yet exited. The call to
                # join should not block indefinitely because there is no
                # other work done outside the while loop in self._run.
                try:
                    self._thread.join()
                except ReferenceError:
                    # Thread terminated.
                    pass
            self._thread_will_exit = False
            self._stopped = False
        started = False
        try:
            started = self._thread and self._thread.is_alive()
        except ReferenceError:
            # Thread terminated.
            pass

        if not started:
            thread = threading.Thread(target=self._run, name=self._name)
            thread.daemon = True
            self._thread = weakref.proxy(thread)
            _register_executor(self)
            thread.start()

    def close(self, dummy=None):
        """Stop. To restart, call open().

        The dummy parameter allows an executor's close method to be a weakref
        callback; see monitor.py.
        """
        self._stopped = True

    def join(self, timeout=None):
        if self._thread is not None:
            try:
                self._thread.join(timeout)
            except (ReferenceError, RuntimeError):
                # Thread already terminated, or not yet started.
                pass

    def wake(self):
        """Execute the target function soon."""
        self._event = True

    def update_interval(self, new_interval):
        self._interval = new_interval

    def __should_stop(self):
        with self._lock:
            if self._stopped:
                self._thread_will_exit = True
                return True
            return False

    def _run(self):
        while not self.__should_stop():
            try:
                if not self._target():
                    self._stopped = True
                    break
            except:
                with self._lock:
                    self._stopped = True
                    self._thread_will_exit = True

                raise

            deadline = _time() + self._interval

            while not self._stopped and _time() < deadline:
                time.sleep(self._min_interval)
                if self._event:
                    break  # Early wake.

            self._event = False


# _EXECUTORS has a weakref to each running PeriodicExecutor. Once started,
# an executor is kept alive by a strong reference from its thread and perhaps
# from other objects. When the thread dies and all other referrers are freed,
# the executor is freed and removed from _EXECUTORS. If any threads are
# running when the interpreter begins to shut down, we try to halt and join
# them to avoid spurious errors.
_EXECUTORS = set()


def _register_executor(executor):
    ref = weakref.ref(executor, _on_executor_deleted)
    _EXECUTORS.add(ref)


def _on_executor_deleted(ref):
    _EXECUTORS.remove(ref)


def _shutdown_executors():
    if _EXECUTORS is None:
        return

    # Copy the set. Stopping threads has the side effect of removing executors.
    executors = list(_EXECUTORS)

    # First signal all executors to close...
    for ref in executors:
        executor = ref()
        if executor:
            executor.close()

    # ...then try to join them.
    for ref in executors:
        executor = ref()
        if executor:
            executor.join(1)

    executor = None

atexit.register(_shutdown_executors)
