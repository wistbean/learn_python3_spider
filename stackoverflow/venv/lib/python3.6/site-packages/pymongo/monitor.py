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

"""Class to monitor a MongoDB server on a background thread."""

import weakref

from pymongo import common, periodic_executor
from pymongo.errors import OperationFailure
from pymongo.monotonic import time as _time
from pymongo.read_preferences import MovingAverage
from pymongo.server_description import ServerDescription
from pymongo.server_type import SERVER_TYPE
from pymongo.srv_resolver import _SrvResolver


class MonitorBase(object):
    def __init__(self, *args, **kwargs):
        """Override this method to create an executor."""
        raise NotImplementedError

    def open(self):
        """Start monitoring, or restart after a fork.

        Multiple calls have no effect.
        """
        self._executor.open()

    def close(self):
        """Close and stop monitoring.

        open() restarts the monitor after closing.
        """
        self._executor.close()

    def join(self, timeout=None):
        """Wait for the monitor to stop."""
        self._executor.join(timeout)

    def request_check(self):
        """If the monitor is sleeping, wake it soon."""
        self._executor.wake()


class Monitor(MonitorBase):
    def __init__(
            self,
            server_description,
            topology,
            pool,
            topology_settings):
        """Class to monitor a MongoDB server on a background thread.

        Pass an initial ServerDescription, a Topology, a Pool, and
        TopologySettings.

        The Topology is weakly referenced. The Pool must be exclusive to this
        Monitor.
        """
        self._server_description = server_description
        self._pool = pool
        self._settings = topology_settings
        self._avg_round_trip_time = MovingAverage()
        self._listeners = self._settings._pool_options.event_listeners
        pub = self._listeners is not None
        self._publish = pub and self._listeners.enabled_for_server_heartbeat

        # We strongly reference the executor and it weakly references us via
        # this closure. When the monitor is freed, stop the executor soon.
        def target():
            monitor = self_ref()
            if monitor is None:
                return False  # Stop the executor.
            Monitor._run(monitor)
            return True

        executor = periodic_executor.PeriodicExecutor(
            interval=self._settings.heartbeat_frequency,
            min_interval=common.MIN_HEARTBEAT_INTERVAL,
            target=target,
            name="pymongo_server_monitor_thread")

        self._executor = executor

        # Avoid cycles. When self or topology is freed, stop executor soon.
        self_ref = weakref.ref(self, executor.close)
        self._topology = weakref.proxy(topology, executor.close)

    def close(self):
        super(Monitor, self).close()

        # Increment the pool_id and maybe close the socket. If the executor
        # thread has the socket checked out, it will be closed when checked in.
        self._pool.reset()

    def _run(self):
        try:
            self._server_description = self._check_with_retry()
            self._topology.on_change(self._server_description)
        except ReferenceError:
            # Topology was garbage-collected.
            self.close()

    def _check_with_retry(self):
        """Call ismaster once or twice. Reset server's pool on error.

        Returns a ServerDescription.
        """
        # According to the spec, if an ismaster call fails we reset the
        # server's pool. If a server was once connected, change its type
        # to Unknown only after retrying once.
        address = self._server_description.address
        retry = True
        if self._server_description.server_type == SERVER_TYPE.Unknown:
            retry = False

        start = _time()
        try:
            return self._check_once()
        except ReferenceError:
            raise
        except Exception as error:
            error_time = _time() - start
            if self._publish:
                self._listeners.publish_server_heartbeat_failed(
                    address, error_time, error)
            self._topology.reset_pool(address)
            default = ServerDescription(address, error=error)
            if not retry:
                self._avg_round_trip_time.reset()
                # Server type defaults to Unknown.
                return default

            # Try a second and final time. If it fails return original error.
            # Always send metadata: this is a new connection.
            start = _time()
            try:
                return self._check_once()
            except ReferenceError:
                raise
            except Exception as error:
                error_time = _time() - start
                if self._publish:
                    self._listeners.publish_server_heartbeat_failed(
                        address, error_time, error)
                self._avg_round_trip_time.reset()
                return default

    def _check_once(self):
        """A single attempt to call ismaster.

        Returns a ServerDescription, or raises an exception.
        """
        address = self._server_description.address
        if self._publish:
            self._listeners.publish_server_heartbeat_started(address)
        with self._pool.get_socket({}) as sock_info:
            response, round_trip_time = self._check_with_socket(sock_info)
            self._avg_round_trip_time.add_sample(round_trip_time)
            sd = ServerDescription(
                address=address,
                ismaster=response,
                round_trip_time=self._avg_round_trip_time.get())
            if self._publish:
                self._listeners.publish_server_heartbeat_succeeded(
                    address, round_trip_time, response)

            return sd

    def _check_with_socket(self, sock_info):
        """Return (IsMaster, round_trip_time).

        Can raise ConnectionFailure or OperationFailure.
        """
        start = _time()
        try:
            return (sock_info.ismaster(self._pool.opts.metadata,
                                       self._topology.max_cluster_time()),
                    _time() - start)
        except OperationFailure as exc:
            # Update max cluster time even when isMaster fails.
            self._topology.receive_cluster_time(
                exc.details.get('$clusterTime'))
            raise


class SrvMonitor(MonitorBase):
    def __init__(self, topology, topology_settings):
        """Class to poll SRV records on a background thread.

        Pass a Topology and a TopologySettings.

        The Topology is weakly referenced.
        """
        self._settings = topology_settings
        self._seedlist = self._settings._seeds
        self._fqdn = self._settings.fqdn

        # We strongly reference the executor and it weakly references us via
        # this closure. When the monitor is freed, stop the executor soon.
        def target():
            monitor = self_ref()
            if monitor is None:
                return False  # Stop the executor.
            SrvMonitor._run(monitor)
            return True

        executor = periodic_executor.PeriodicExecutor(
            interval=common.MIN_SRV_RESCAN_INTERVAL,
            min_interval=self._settings.heartbeat_frequency,
            target=target,
            name="pymongo_srv_polling_thread")

        self._executor = executor

        # Avoid cycles. When self or topology is freed, stop executor soon.
        self_ref = weakref.ref(self, executor.close)
        self._topology = weakref.proxy(topology, executor.close)

    def _run(self):
        seedlist = self._get_seedlist()
        if seedlist:
            self._seedlist = seedlist
            try:
                self._topology.on_srv_update(self._seedlist)
            except ReferenceError:
                # Topology was garbage-collected.
                self.close()

    def _get_seedlist(self):
        """Poll SRV records for a seedlist.

        Returns a list of ServerDescriptions.
        """
        try:
            seedlist, ttl = _SrvResolver(self._fqdn).get_hosts_and_min_ttl()
            if len(seedlist) == 0:
                # As per the spec: this should be treated as a failure.
                raise Exception
        except Exception:
            # As per the spec, upon encountering an error:
            # - An error must not be raised
            # - SRV records must be rescanned every heartbeatFrequencyMS
            # - Topology must be left unchanged
            self.request_check()
            return None
        else:
            self._executor.update_interval(
                max(ttl, common.MIN_SRV_RESCAN_INTERVAL))
            return seedlist
