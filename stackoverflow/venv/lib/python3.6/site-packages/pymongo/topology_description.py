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

"""Represent a deployment of MongoDB servers."""

from collections import namedtuple

from pymongo import common
from pymongo.errors import ConfigurationError
from pymongo.read_preferences import ReadPreference
from pymongo.server_description import ServerDescription
from pymongo.server_selectors import Selection
from pymongo.server_type import SERVER_TYPE


# Enumeration for various kinds of MongoDB cluster topologies.
TOPOLOGY_TYPE = namedtuple('TopologyType', ['Single', 'ReplicaSetNoPrimary',
                                            'ReplicaSetWithPrimary', 'Sharded',
                                            'Unknown'])(*range(5))

# Topologies compatible with SRV record polling.
SRV_POLLING_TOPOLOGIES = (TOPOLOGY_TYPE.Unknown, TOPOLOGY_TYPE.Sharded)


class TopologyDescription(object):
    def __init__(self,
                 topology_type,
                 server_descriptions,
                 replica_set_name,
                 max_set_version,
                 max_election_id,
                 topology_settings):
        """Representation of a deployment of MongoDB servers.

        :Parameters:
          - `topology_type`: initial type
          - `server_descriptions`: dict of (address, ServerDescription) for
            all seeds
          - `replica_set_name`: replica set name or None
          - `max_set_version`: greatest setVersion seen from a primary, or None
          - `max_election_id`: greatest electionId seen from a primary, or None
          - `topology_settings`: a TopologySettings
        """
        self._topology_type = topology_type
        self._replica_set_name = replica_set_name
        self._server_descriptions = server_descriptions
        self._max_set_version = max_set_version
        self._max_election_id = max_election_id

        # The heartbeat_frequency is used in staleness estimates.
        self._topology_settings = topology_settings

        # Is PyMongo compatible with all servers' wire protocols?
        self._incompatible_err = None

        for s in self._server_descriptions.values():
            if not s.is_server_type_known:
                continue

            # s.min/max_wire_version is the server's wire protocol.
            # MIN/MAX_SUPPORTED_WIRE_VERSION is what PyMongo supports.
            server_too_new = (
                # Server too new.
                s.min_wire_version is not None
                and s.min_wire_version > common.MAX_SUPPORTED_WIRE_VERSION)

            server_too_old = (
                # Server too old.
                s.max_wire_version is not None
                and s.max_wire_version < common.MIN_SUPPORTED_WIRE_VERSION)

            if server_too_new:
                self._incompatible_err = (
                    "Server at %s:%d requires wire version %d, but this "
                    "version of PyMongo only supports up to %d."
                    % (s.address[0], s.address[1],
                       s.min_wire_version, common.MAX_SUPPORTED_WIRE_VERSION))

            elif server_too_old:
                self._incompatible_err = (
                    "Server at %s:%d reports wire version %d, but this "
                    "version of PyMongo requires at least %d (MongoDB %s)."
                    % (s.address[0], s.address[1],
                       s.max_wire_version,
                       common.MIN_SUPPORTED_WIRE_VERSION,
                       common.MIN_SUPPORTED_SERVER_VERSION))

                break

        # Server Discovery And Monitoring Spec: Whenever a client updates the
        # TopologyDescription from an ismaster response, it MUST set
        # TopologyDescription.logicalSessionTimeoutMinutes to the smallest
        # logicalSessionTimeoutMinutes value among ServerDescriptions of all
        # data-bearing server types. If any have a null
        # logicalSessionTimeoutMinutes, then
        # TopologyDescription.logicalSessionTimeoutMinutes MUST be set to null.
        readable_servers = self.readable_servers
        if not readable_servers:
            self._ls_timeout_minutes = None
        elif any(s.logical_session_timeout_minutes is None
                 for s in readable_servers):
            self._ls_timeout_minutes = None
        else:
            self._ls_timeout_minutes = min(s.logical_session_timeout_minutes
                                           for s in readable_servers)

    def check_compatible(self):
        """Raise ConfigurationError if any server is incompatible.

        A server is incompatible if its wire protocol version range does not
        overlap with PyMongo's.
        """
        if self._incompatible_err:
            raise ConfigurationError(self._incompatible_err)

    def has_server(self, address):
        return address in self._server_descriptions

    def reset_server(self, address):
        """A copy of this description, with one server marked Unknown."""
        return updated_topology_description(self, ServerDescription(address))

    def reset(self):
        """A copy of this description, with all servers marked Unknown."""
        if self._topology_type == TOPOLOGY_TYPE.ReplicaSetWithPrimary:
            topology_type = TOPOLOGY_TYPE.ReplicaSetNoPrimary
        else:
            topology_type = self._topology_type

        # The default ServerDescription's type is Unknown.
        sds = dict((address, ServerDescription(address))
                   for address in self._server_descriptions)

        return TopologyDescription(
            topology_type,
            sds,
            self._replica_set_name,
            self._max_set_version,
            self._max_election_id,
            self._topology_settings)

    def server_descriptions(self):
        """Dict of (address,
        :class:`~pymongo.server_description.ServerDescription`)."""
        return self._server_descriptions.copy()

    @property
    def topology_type(self):
        """The type of this topology."""
        return self._topology_type

    @property
    def topology_type_name(self):
        """The topology type as a human readable string.

        .. versionadded:: 3.4
        """
        return TOPOLOGY_TYPE._fields[self._topology_type]

    @property
    def replica_set_name(self):
        """The replica set name."""
        return self._replica_set_name

    @property
    def max_set_version(self):
        """Greatest setVersion seen from a primary, or None."""
        return self._max_set_version

    @property
    def max_election_id(self):
        """Greatest electionId seen from a primary, or None."""
        return self._max_election_id

    @property
    def logical_session_timeout_minutes(self):
        """Minimum logical session timeout, or None."""
        return self._ls_timeout_minutes

    @property
    def known_servers(self):
        """List of Servers of types besides Unknown."""
        return [s for s in self._server_descriptions.values()
                if s.is_server_type_known]

    @property
    def has_known_servers(self):
        """Whether there are any Servers of types besides Unknown."""
        return any(s for s in self._server_descriptions.values()
                   if s.is_server_type_known)

    @property
    def readable_servers(self):
        """List of readable Servers."""
        return [s for s in self._server_descriptions.values() if s.is_readable]

    @property
    def common_wire_version(self):
        """Minimum of all servers' max wire versions, or None."""
        servers = self.known_servers
        if servers:
            return min(s.max_wire_version for s in self.known_servers)

        return None

    @property
    def heartbeat_frequency(self):
        return self._topology_settings.heartbeat_frequency

    def apply_selector(self, selector, address, custom_selector=None):

        def apply_local_threshold(selection):
            if not selection:
                return []

            settings = self._topology_settings

            # Round trip time in seconds.
            fastest = min(
                s.round_trip_time for s in selection.server_descriptions)
            threshold = settings.local_threshold_ms / 1000.0
            return [s for s in selection.server_descriptions
                    if (s.round_trip_time - fastest) <= threshold]

        if getattr(selector, 'min_wire_version', 0):
            common_wv = self.common_wire_version
            if common_wv and common_wv < selector.min_wire_version:
                raise ConfigurationError(
                    "%s requires min wire version %d, but topology's min"
                    " wire version is %d" % (selector,
                                             selector.min_wire_version,
                                             common_wv))

        if self.topology_type == TOPOLOGY_TYPE.Single:
            # Ignore selectors for standalone.
            return self.known_servers
        elif address:
            # Ignore selectors when explicit address is requested.
            description = self.server_descriptions().get(address)
            return [description] if description else []
        elif self.topology_type == TOPOLOGY_TYPE.Sharded:
            # Ignore read preference.
            selection = Selection.from_topology_description(self)
        else:
            selection = selector(Selection.from_topology_description(self))

        # Apply custom selector followed by localThresholdMS.
        if custom_selector is not None and selection:
            selection = selection.with_server_descriptions(
                custom_selector(selection.server_descriptions))
        return apply_local_threshold(selection)

    def has_readable_server(self, read_preference=ReadPreference.PRIMARY):
        """Does this topology have any readable servers available matching the
        given read preference?

        :Parameters:
          - `read_preference`: an instance of a read preference from
            :mod:`~pymongo.read_preferences`. Defaults to
            :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY`.

        .. note:: When connected directly to a single server this method
          always returns ``True``.

        .. versionadded:: 3.4
        """
        common.validate_read_preference("read_preference", read_preference)
        return any(self.apply_selector(read_preference, None))

    def has_writable_server(self):
        """Does this topology have a writable server available?

        .. note:: When connected directly to a single server this method
          always returns ``True``.

        .. versionadded:: 3.4
        """
        return self.has_readable_server(ReadPreference.PRIMARY)


# If topology type is Unknown and we receive an ismaster response, what should
# the new topology type be?
_SERVER_TYPE_TO_TOPOLOGY_TYPE = {
    SERVER_TYPE.Mongos: TOPOLOGY_TYPE.Sharded,
    SERVER_TYPE.RSPrimary: TOPOLOGY_TYPE.ReplicaSetWithPrimary,
    SERVER_TYPE.RSSecondary: TOPOLOGY_TYPE.ReplicaSetNoPrimary,
    SERVER_TYPE.RSArbiter: TOPOLOGY_TYPE.ReplicaSetNoPrimary,
    SERVER_TYPE.RSOther: TOPOLOGY_TYPE.ReplicaSetNoPrimary,
}


def updated_topology_description(topology_description, server_description):
    """Return an updated copy of a TopologyDescription.

    :Parameters:
      - `topology_description`: the current TopologyDescription
      - `server_description`: a new ServerDescription that resulted from
        an ismaster call

    Called after attempting (successfully or not) to call ismaster on the
    server at server_description.address. Does not modify topology_description.
    """
    address = server_description.address

    # These values will be updated, if necessary, to form the new
    # TopologyDescription.
    topology_type = topology_description.topology_type
    set_name = topology_description.replica_set_name
    max_set_version = topology_description.max_set_version
    max_election_id = topology_description.max_election_id
    server_type = server_description.server_type

    # Don't mutate the original dict of server descriptions; copy it.
    sds = topology_description.server_descriptions()

    # Replace this server's description with the new one.
    sds[address] = server_description

    if topology_type == TOPOLOGY_TYPE.Single:
        # Single type never changes.
        return TopologyDescription(
            TOPOLOGY_TYPE.Single,
            sds,
            set_name,
            max_set_version,
            max_election_id,
            topology_description._topology_settings)

    if topology_type == TOPOLOGY_TYPE.Unknown:
        if server_type == SERVER_TYPE.Standalone:
            sds.pop(address)

        elif server_type not in (SERVER_TYPE.Unknown, SERVER_TYPE.RSGhost):
            topology_type = _SERVER_TYPE_TO_TOPOLOGY_TYPE[server_type]

    if topology_type == TOPOLOGY_TYPE.Sharded:
        if server_type not in (SERVER_TYPE.Mongos, SERVER_TYPE.Unknown):
            sds.pop(address)

    elif topology_type == TOPOLOGY_TYPE.ReplicaSetNoPrimary:
        if server_type in (SERVER_TYPE.Standalone, SERVER_TYPE.Mongos):
            sds.pop(address)

        elif server_type == SERVER_TYPE.RSPrimary:
            (topology_type,
             set_name,
             max_set_version,
             max_election_id) = _update_rs_from_primary(sds,
                                                        set_name,
                                                        server_description,
                                                        max_set_version,
                                                        max_election_id)

        elif server_type in (
                SERVER_TYPE.RSSecondary,
                SERVER_TYPE.RSArbiter,
                SERVER_TYPE.RSOther):
            topology_type, set_name = _update_rs_no_primary_from_member(
                sds, set_name, server_description)

    elif topology_type == TOPOLOGY_TYPE.ReplicaSetWithPrimary:
        if server_type in (SERVER_TYPE.Standalone, SERVER_TYPE.Mongos):
            sds.pop(address)
            topology_type = _check_has_primary(sds)

        elif server_type == SERVER_TYPE.RSPrimary:
            (topology_type,
             set_name,
             max_set_version,
             max_election_id) = _update_rs_from_primary(sds,
                                                        set_name,
                                                        server_description,
                                                        max_set_version,
                                                        max_election_id)

        elif server_type in (
                SERVER_TYPE.RSSecondary,
                SERVER_TYPE.RSArbiter,
                SERVER_TYPE.RSOther):
            topology_type = _update_rs_with_primary_from_member(
                sds, set_name, server_description)

        else:
            # Server type is Unknown or RSGhost: did we just lose the primary?
            topology_type = _check_has_primary(sds)

    # Return updated copy.
    return TopologyDescription(topology_type,
                               sds,
                               set_name,
                               max_set_version,
                               max_election_id,
                               topology_description._topology_settings)


def _updated_topology_description_srv_polling(topology_description, seedlist):
    """Return an updated copy of a TopologyDescription.

    :Parameters:
      - `topology_description`: the current TopologyDescription
      - `seedlist`: a list of new seeds new ServerDescription that resulted from
        an ismaster call
    """
    # Create a copy of the server descriptions.
    sds = topology_description.server_descriptions()

    # If seeds haven't changed, don't do anything.
    if set(sds.keys()) == set(seedlist):
        return topology_description

    # Add SDs corresponding to servers recently added to the SRV record.
    for address in seedlist:
        if address not in sds:
            sds[address] = ServerDescription(address)

    # Remove SDs corresponding to servers no longer part of the SRV record.
    for address in list(sds.keys()):
        if address not in seedlist:
            sds.pop(address)

    return TopologyDescription(
        topology_description.topology_type,
        sds,
        topology_description.replica_set_name,
        topology_description.max_set_version,
        topology_description.max_election_id,
        topology_description._topology_settings)


def _update_rs_from_primary(
        sds,
        replica_set_name,
        server_description,
        max_set_version,
        max_election_id):
    """Update topology description from a primary's ismaster response.

    Pass in a dict of ServerDescriptions, current replica set name, the
    ServerDescription we are processing, and the TopologyDescription's
    max_set_version and max_election_id if any.

    Returns (new topology type, new replica_set_name, new max_set_version,
    new max_election_id).
    """
    if replica_set_name is None:
        replica_set_name = server_description.replica_set_name

    elif replica_set_name != server_description.replica_set_name:
        # We found a primary but it doesn't have the replica_set_name
        # provided by the user.
        sds.pop(server_description.address)
        return (_check_has_primary(sds),
                replica_set_name,
                max_set_version,
                max_election_id)

    max_election_tuple = max_set_version, max_election_id
    if None not in server_description.election_tuple:
        if (None not in max_election_tuple and
                max_election_tuple > server_description.election_tuple):

            # Stale primary, set to type Unknown.
            address = server_description.address
            sds[address] = ServerDescription(address)
            return (_check_has_primary(sds),
                    replica_set_name,
                    max_set_version,
                    max_election_id)

        max_election_id = server_description.election_id

    if (server_description.set_version is not None and
        (max_set_version is None or
            server_description.set_version > max_set_version)):

        max_set_version = server_description.set_version

    # We've heard from the primary. Is it the same primary as before?
    for server in sds.values():
        if (server.server_type is SERVER_TYPE.RSPrimary
                and server.address != server_description.address):

            # Reset old primary's type to Unknown.
            sds[server.address] = ServerDescription(server.address)

            # There can be only one prior primary.
            break

    # Discover new hosts from this primary's response.
    for new_address in server_description.all_hosts:
        if new_address not in sds:
            sds[new_address] = ServerDescription(new_address)

    # Remove hosts not in the response.
    for addr in set(sds) - server_description.all_hosts:
        sds.pop(addr)

    # If the host list differs from the seed list, we may not have a primary
    # after all.
    return (_check_has_primary(sds),
            replica_set_name,
            max_set_version,
            max_election_id)


def _update_rs_with_primary_from_member(
        sds,
        replica_set_name,
        server_description):
    """RS with known primary. Process a response from a non-primary.

    Pass in a dict of ServerDescriptions, current replica set name, and the
    ServerDescription we are processing.

    Returns new topology type.
    """
    assert replica_set_name is not None

    if replica_set_name != server_description.replica_set_name:
        sds.pop(server_description.address)
    elif (server_description.me and
          server_description.address != server_description.me):
        sds.pop(server_description.address)

    # Had this member been the primary?
    return _check_has_primary(sds)


def _update_rs_no_primary_from_member(
        sds,
        replica_set_name,
        server_description):
    """RS without known primary. Update from a non-primary's response.

    Pass in a dict of ServerDescriptions, current replica set name, and the
    ServerDescription we are processing.

    Returns (new topology type, new replica_set_name).
    """
    topology_type = TOPOLOGY_TYPE.ReplicaSetNoPrimary
    if replica_set_name is None:
        replica_set_name = server_description.replica_set_name

    elif replica_set_name != server_description.replica_set_name:
        sds.pop(server_description.address)
        return topology_type, replica_set_name

    # This isn't the primary's response, so don't remove any servers
    # it doesn't report. Only add new servers.
    for address in server_description.all_hosts:
        if address not in sds:
            sds[address] = ServerDescription(address)

    if (server_description.me and
            server_description.address != server_description.me):
        sds.pop(server_description.address)

    return topology_type, replica_set_name


def _check_has_primary(sds):
    """Current topology type is ReplicaSetWithPrimary. Is primary still known?

    Pass in a dict of ServerDescriptions.

    Returns new topology type.
    """
    for s in sds.values():
        if s.server_type == SERVER_TYPE.RSPrimary:
            return TOPOLOGY_TYPE.ReplicaSetWithPrimary
    else:
        return TOPOLOGY_TYPE.ReplicaSetNoPrimary
