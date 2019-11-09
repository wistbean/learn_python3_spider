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

"""Tools to parse mongo client options."""

from bson.codec_options import _parse_codec_options
from pymongo.auth import _build_credentials_tuple
from pymongo.common import validate_boolean
from pymongo import common
from pymongo.compression_support import CompressionSettings
from pymongo.errors import ConfigurationError
from pymongo.monitoring import _EventListeners
from pymongo.pool import PoolOptions
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import (make_read_preference,
                                      read_pref_mode_from_name)
from pymongo.server_selectors import any_server_selector
from pymongo.ssl_support import get_ssl_context
from pymongo.write_concern import WriteConcern


def _parse_credentials(username, password, database, options):
    """Parse authentication credentials."""
    mechanism = options.get('authmechanism', 'DEFAULT' if username else None)
    source = options.get('authsource')
    if username or mechanism:
        return _build_credentials_tuple(
            mechanism, source, username, password, options, database)
    return None


def _parse_read_preference(options):
    """Parse read preference options."""
    if 'read_preference' in options:
        return options['read_preference']

    name = options.get('readpreference', 'primary')
    mode = read_pref_mode_from_name(name)
    tags = options.get('readpreferencetags')
    max_staleness = options.get('maxstalenessseconds', -1)
    return make_read_preference(mode, tags, max_staleness)


def _parse_write_concern(options):
    """Parse write concern options."""
    concern = options.get('w')
    wtimeout = options.get('wtimeoutms')
    j = options.get('journal')
    fsync = options.get('fsync')
    return WriteConcern(concern, wtimeout, j, fsync)


def _parse_read_concern(options):
    """Parse read concern options."""
    concern = options.get('readconcernlevel')
    return ReadConcern(concern)


def _parse_ssl_options(options):
    """Parse ssl options."""
    use_ssl = options.get('ssl')
    if use_ssl is not None:
        validate_boolean('ssl', use_ssl)

    certfile = options.get('ssl_certfile')
    keyfile = options.get('ssl_keyfile')
    passphrase = options.get('ssl_pem_passphrase')
    ca_certs = options.get('ssl_ca_certs')
    cert_reqs = options.get('ssl_cert_reqs')
    match_hostname = options.get('ssl_match_hostname', True)
    crlfile = options.get('ssl_crlfile')

    ssl_kwarg_keys = [k for k in options
                      if k.startswith('ssl_') and options[k]]
    if use_ssl == False and ssl_kwarg_keys:
        raise ConfigurationError("ssl has not been enabled but the "
                                 "following ssl parameters have been set: "
                                 "%s. Please set `ssl=True` or remove."
                                 % ', '.join(ssl_kwarg_keys))

    if ssl_kwarg_keys and use_ssl is None:
        # ssl options imply ssl = True
        use_ssl = True

    if use_ssl is True:
        ctx = get_ssl_context(
            certfile,
            keyfile,
            passphrase,
            ca_certs,
            cert_reqs,
            crlfile,
            match_hostname)
        return ctx, match_hostname
    return None, match_hostname


def _parse_pool_options(options):
    """Parse connection pool options."""
    max_pool_size = options.get('maxpoolsize', common.MAX_POOL_SIZE)
    min_pool_size = options.get('minpoolsize', common.MIN_POOL_SIZE)
    max_idle_time_seconds = options.get(
        'maxidletimems', common.MAX_IDLE_TIME_SEC)
    if max_pool_size is not None and min_pool_size > max_pool_size:
        raise ValueError("minPoolSize must be smaller or equal to maxPoolSize")
    connect_timeout = options.get('connecttimeoutms', common.CONNECT_TIMEOUT)
    socket_keepalive = options.get('socketkeepalive', True)
    socket_timeout = options.get('sockettimeoutms')
    wait_queue_timeout = options.get(
        'waitqueuetimeoutms', common.WAIT_QUEUE_TIMEOUT)
    wait_queue_multiple = options.get('waitqueuemultiple')
    event_listeners = options.get('event_listeners')
    appname = options.get('appname')
    driver = options.get('driver')
    compression_settings = CompressionSettings(
        options.get('compressors', []),
        options.get('zlibcompressionlevel', -1))
    ssl_context, ssl_match_hostname = _parse_ssl_options(options)
    return PoolOptions(max_pool_size,
                       min_pool_size,
                       max_idle_time_seconds,
                       connect_timeout, socket_timeout,
                       wait_queue_timeout, wait_queue_multiple,
                       ssl_context, ssl_match_hostname, socket_keepalive,
                       _EventListeners(event_listeners),
                       appname,
                       driver,
                       compression_settings)


class ClientOptions(object):

    """ClientOptions"""

    def __init__(self, username, password, database, options):
        self.__options = options

        self.__codec_options = _parse_codec_options(options)
        self.__credentials = _parse_credentials(
            username, password, database, options)
        self.__local_threshold_ms = options.get(
            'localthresholdms', common.LOCAL_THRESHOLD_MS)
        # self.__server_selection_timeout is in seconds. Must use full name for
        # common.SERVER_SELECTION_TIMEOUT because it is set directly by tests.
        self.__server_selection_timeout = options.get(
            'serverselectiontimeoutms', common.SERVER_SELECTION_TIMEOUT)
        self.__pool_options = _parse_pool_options(options)
        self.__read_preference = _parse_read_preference(options)
        self.__replica_set_name = options.get('replicaset')
        self.__write_concern = _parse_write_concern(options)
        self.__read_concern = _parse_read_concern(options)
        self.__connect = options.get('connect')
        self.__heartbeat_frequency = options.get(
            'heartbeatfrequencyms', common.HEARTBEAT_FREQUENCY)
        self.__retry_writes = options.get('retrywrites', common.RETRY_WRITES)
        self.__retry_reads = options.get('retryreads', common.RETRY_READS)
        self.__server_selector = options.get(
            'server_selector', any_server_selector)
        self.__auto_encryption_opts = options.get('auto_encryption_opts')

    @property
    def _options(self):
        """The original options used to create this ClientOptions."""
        return self.__options

    @property
    def connect(self):
        """Whether to begin discovering a MongoDB topology automatically."""
        return self.__connect

    @property
    def codec_options(self):
        """A :class:`~bson.codec_options.CodecOptions` instance."""
        return self.__codec_options

    @property
    def credentials(self):
        """A :class:`~pymongo.auth.MongoCredentials` instance or None."""
        return self.__credentials

    @property
    def local_threshold_ms(self):
        """The local threshold for this instance."""
        return self.__local_threshold_ms

    @property
    def server_selection_timeout(self):
        """The server selection timeout for this instance in seconds."""
        return self.__server_selection_timeout

    @property
    def server_selector(self):
        return self.__server_selector

    @property
    def heartbeat_frequency(self):
        """The monitoring frequency in seconds."""
        return self.__heartbeat_frequency

    @property
    def pool_options(self):
        """A :class:`~pymongo.pool.PoolOptions` instance."""
        return self.__pool_options

    @property
    def read_preference(self):
        """A read preference instance."""
        return self.__read_preference

    @property
    def replica_set_name(self):
        """Replica set name or None."""
        return self.__replica_set_name

    @property
    def write_concern(self):
        """A :class:`~pymongo.write_concern.WriteConcern` instance."""
        return self.__write_concern

    @property
    def read_concern(self):
        """A :class:`~pymongo.read_concern.ReadConcern` instance."""
        return self.__read_concern

    @property
    def retry_writes(self):
        """If this instance should retry supported write operations."""
        return self.__retry_writes

    @property
    def retry_reads(self):
        """If this instance should retry supported read operations."""
        return self.__retry_reads

    @property
    def auto_encryption_opts(self):
        """A :class:`~pymongo.encryption.AutoEncryptionOpts` or None."""
        return self.__auto_encryption_opts
