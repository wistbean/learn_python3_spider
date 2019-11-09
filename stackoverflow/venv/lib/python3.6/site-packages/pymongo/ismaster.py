# Copyright 2014-present MongoDB, Inc.
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

"""Parse a response to the 'ismaster' command."""

import itertools

from bson.py3compat import imap
from pymongo import common
from pymongo.server_type import SERVER_TYPE


def _get_server_type(doc):
    """Determine the server type from an ismaster response."""
    if not doc.get('ok'):
        return SERVER_TYPE.Unknown

    if doc.get('isreplicaset'):
        return SERVER_TYPE.RSGhost
    elif doc.get('setName'):
        if doc.get('hidden'):
            return SERVER_TYPE.RSOther
        elif doc.get('ismaster'):
            return SERVER_TYPE.RSPrimary
        elif doc.get('secondary'):
            return SERVER_TYPE.RSSecondary
        elif doc.get('arbiterOnly'):
            return SERVER_TYPE.RSArbiter
        else:
            return SERVER_TYPE.RSOther
    elif doc.get('msg') == 'isdbgrid':
        return SERVER_TYPE.Mongos
    else:
        return SERVER_TYPE.Standalone


class IsMaster(object):
    __slots__ = ('_doc', '_server_type', '_is_writable', '_is_readable')

    def __init__(self, doc):
        """Parse an ismaster response from the server."""
        self._server_type = _get_server_type(doc)
        self._doc = doc
        self._is_writable = self._server_type in (
            SERVER_TYPE.RSPrimary,
            SERVER_TYPE.Standalone,
            SERVER_TYPE.Mongos)

        self._is_readable = (
            self.server_type == SERVER_TYPE.RSSecondary
            or self._is_writable)

    @property
    def document(self):
        """The complete ismaster command response document.

        .. versionadded:: 3.4
        """
        return self._doc.copy()

    @property
    def server_type(self):
        return self._server_type

    @property
    def all_hosts(self):
        """List of hosts, passives, and arbiters known to this server."""
        return set(imap(common.clean_node, itertools.chain(
            self._doc.get('hosts', []),
            self._doc.get('passives', []),
            self._doc.get('arbiters', []))))

    @property
    def tags(self):
        """Replica set member tags or empty dict."""
        return self._doc.get('tags', {})

    @property
    def primary(self):
        """This server's opinion about who the primary is, or None."""
        if self._doc.get('primary'):
            return common.partition_node(self._doc['primary'])
        else:
            return None

    @property
    def replica_set_name(self):
        """Replica set name or None."""
        return self._doc.get('setName')

    @property
    def max_bson_size(self):
        return self._doc.get('maxBsonObjectSize', common.MAX_BSON_SIZE)

    @property
    def max_message_size(self):
        return self._doc.get('maxMessageSizeBytes', 2 * self.max_bson_size)

    @property
    def max_write_batch_size(self):
        return self._doc.get('maxWriteBatchSize', common.MAX_WRITE_BATCH_SIZE)

    @property
    def min_wire_version(self):
        return self._doc.get('minWireVersion', common.MIN_WIRE_VERSION)

    @property
    def max_wire_version(self):
        return self._doc.get('maxWireVersion', common.MAX_WIRE_VERSION)

    @property
    def set_version(self):
        return self._doc.get('setVersion')

    @property
    def election_id(self):
        return self._doc.get('electionId')

    @property
    def cluster_time(self):
        return self._doc.get('$clusterTime')

    @property
    def logical_session_timeout_minutes(self):
        return self._doc.get('logicalSessionTimeoutMinutes')

    @property
    def is_writable(self):
        return self._is_writable

    @property
    def is_readable(self):
        return self._is_readable

    @property
    def me(self):
        me = self._doc.get('me')
        if me:
            return common.clean_node(me)

    @property
    def last_write_date(self):
        return self._doc.get('lastWrite', {}).get('lastWriteDate')

    @property
    def compressors(self):
        return self._doc.get('compression')
