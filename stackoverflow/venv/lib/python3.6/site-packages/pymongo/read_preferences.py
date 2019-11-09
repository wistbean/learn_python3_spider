# Copyright 2012-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License",
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

"""Utilities for choosing which member of a replica set to read from."""

from bson.py3compat import abc, integer_types
from pymongo import max_staleness_selectors
from pymongo.errors import ConfigurationError
from pymongo.server_selectors import (member_with_tags_server_selector,
                                      secondary_with_tags_server_selector)


_PRIMARY = 0
_PRIMARY_PREFERRED = 1
_SECONDARY = 2
_SECONDARY_PREFERRED = 3
_NEAREST = 4


_MONGOS_MODES = (
    'primary',
    'primaryPreferred',
    'secondary',
    'secondaryPreferred',
    'nearest',
)


def _validate_tag_sets(tag_sets):
    """Validate tag sets for a MongoReplicaSetClient.
    """
    if tag_sets is None:
        return tag_sets

    if not isinstance(tag_sets, list):
        raise TypeError((
            "Tag sets %r invalid, must be a list") % (tag_sets,))
    if len(tag_sets) == 0:
        raise ValueError((
            "Tag sets %r invalid, must be None or contain at least one set of"
            " tags") % (tag_sets,))

    for tags in tag_sets:
        if not isinstance(tags, abc.Mapping):
            raise TypeError(
                "Tag set %r invalid, must be an instance of dict, "
                "bson.son.SON or other type that inherits from "
                "collection.Mapping" % (tags,))

    return tag_sets


def _invalid_max_staleness_msg(max_staleness):
    return ("maxStalenessSeconds must be a positive integer, not %s" %
            max_staleness)


# Some duplication with common.py to avoid import cycle.
def _validate_max_staleness(max_staleness):
    """Validate max_staleness."""
    if max_staleness == -1:
        return -1

    if not isinstance(max_staleness, integer_types):
        raise TypeError(_invalid_max_staleness_msg(max_staleness))

    if max_staleness <= 0:
        raise ValueError(_invalid_max_staleness_msg(max_staleness))

    return max_staleness


class _ServerMode(object):
    """Base class for all read preferences.
    """

    __slots__ = ("__mongos_mode", "__mode", "__tag_sets", "__max_staleness")

    def __init__(self, mode, tag_sets=None, max_staleness=-1):
        self.__mongos_mode = _MONGOS_MODES[mode]
        self.__mode = mode
        self.__tag_sets = _validate_tag_sets(tag_sets)
        self.__max_staleness = _validate_max_staleness(max_staleness)

    @property
    def name(self):
        """The name of this read preference.
        """
        return self.__class__.__name__

    @property
    def mongos_mode(self):
        """The mongos mode of this read preference.
        """
        return self.__mongos_mode

    @property
    def document(self):
        """Read preference as a document.
        """
        doc = {'mode': self.__mongos_mode}
        if self.__tag_sets not in (None, [{}]):
            doc['tags'] = self.__tag_sets
        if self.__max_staleness != -1:
            doc['maxStalenessSeconds'] = self.__max_staleness
        return doc

    @property
    def mode(self):
        """The mode of this read preference instance.
        """
        return self.__mode

    @property
    def tag_sets(self):
        """Set ``tag_sets`` to a list of dictionaries like [{'dc': 'ny'}] to
        read only from members whose ``dc`` tag has the value ``"ny"``.
        To specify a priority-order for tag sets, provide a list of
        tag sets: ``[{'dc': 'ny'}, {'dc': 'la'}, {}]``. A final, empty tag
        set, ``{}``, means "read from any member that matches the mode,
        ignoring tags." MongoReplicaSetClient tries each set of tags in turn
        until it finds a set of tags with at least one matching member.

           .. seealso:: `Data-Center Awareness
               <http://www.mongodb.org/display/DOCS/Data+Center+Awareness>`_
        """
        return list(self.__tag_sets) if self.__tag_sets else [{}]

    @property
    def max_staleness(self):
        """The maximum estimated length of time (in seconds) a replica set
        secondary can fall behind the primary in replication before it will
        no longer be selected for operations, or -1 for no maximum."""
        return self.__max_staleness

    @property
    def min_wire_version(self):
        """The wire protocol version the server must support.

        Some read preferences impose version requirements on all servers (e.g.
        maxStalenessSeconds requires MongoDB 3.4 / maxWireVersion 5).

        All servers' maxWireVersion must be at least this read preference's
        `min_wire_version`, or the driver raises
        :exc:`~pymongo.errors.ConfigurationError`.
        """
        return 0 if self.__max_staleness == -1 else 5

    def __repr__(self):
        return "%s(tag_sets=%r, max_staleness=%r)" % (
            self.name, self.__tag_sets, self.__max_staleness)

    def __eq__(self, other):
        if isinstance(other, _ServerMode):
            return (self.mode == other.mode and
                    self.tag_sets == other.tag_sets and
                    self.max_staleness == other.max_staleness)
        return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __getstate__(self):
        """Return value of object for pickling.

        Needed explicitly because __slots__() defined.
        """
        return {'mode': self.__mode,
                'tag_sets': self.__tag_sets,
                'max_staleness': self.__max_staleness}

    def __setstate__(self, value):
        """Restore from pickling."""
        self.__mode = value['mode']
        self.__mongos_mode = _MONGOS_MODES[self.__mode]
        self.__tag_sets = _validate_tag_sets(value['tag_sets'])
        self.__max_staleness = _validate_max_staleness(value['max_staleness'])


class Primary(_ServerMode):
    """Primary read preference.

    * When directly connected to one mongod queries are allowed if the server
      is standalone or a replica set primary.
    * When connected to a mongos queries are sent to the primary of a shard.
    * When connected to a replica set queries are sent to the primary of
      the replica set.
    """

    __slots__ = ()

    def __init__(self):
        super(Primary, self).__init__(_PRIMARY)

    def __call__(self, selection):
        """Apply this read preference to a Selection."""
        return selection.primary_selection

    def __repr__(self):
        return "Primary()"

    def __eq__(self, other):
        if isinstance(other, _ServerMode):
            return other.mode == _PRIMARY
        return NotImplemented


class PrimaryPreferred(_ServerMode):
    """PrimaryPreferred read preference.

    * When directly connected to one mongod queries are allowed to standalone
      servers, to a replica set primary, or to replica set secondaries.
    * When connected to a mongos queries are sent to the primary of a shard if
      available, otherwise a shard secondary.
    * When connected to a replica set queries are sent to the primary if
      available, otherwise a secondary.

    :Parameters:
      - `tag_sets`: The :attr:`~tag_sets` to use if the primary is not
        available.
      - `max_staleness`: (integer, in seconds) The maximum estimated
        length of time a replica set secondary can fall behind the primary in
        replication before it will no longer be selected for operations.
        Default -1, meaning no maximum. If it is set, it must be at least
        90 seconds.
    """

    __slots__ = ()

    def __init__(self, tag_sets=None, max_staleness=-1):
        super(PrimaryPreferred, self).__init__(_PRIMARY_PREFERRED,
                                               tag_sets,
                                               max_staleness)

    def __call__(self, selection):
        """Apply this read preference to Selection."""
        if selection.primary:
            return selection.primary_selection
        else:
            return secondary_with_tags_server_selector(
                self.tag_sets,
                max_staleness_selectors.select(
                    self.max_staleness, selection))


class Secondary(_ServerMode):
    """Secondary read preference.

    * When directly connected to one mongod queries are allowed to standalone
      servers, to a replica set primary, or to replica set secondaries.
    * When connected to a mongos queries are distributed among shard
      secondaries. An error is raised if no secondaries are available.
    * When connected to a replica set queries are distributed among
      secondaries. An error is raised if no secondaries are available.

    :Parameters:
      - `tag_sets`: The :attr:`~tag_sets` for this read preference.
      - `max_staleness`: (integer, in seconds) The maximum estimated
        length of time a replica set secondary can fall behind the primary in
        replication before it will no longer be selected for operations.
        Default -1, meaning no maximum. If it is set, it must be at least
        90 seconds.
    """

    __slots__ = ()

    def __init__(self, tag_sets=None, max_staleness=-1):
        super(Secondary, self).__init__(_SECONDARY, tag_sets, max_staleness)

    def __call__(self, selection):
        """Apply this read preference to Selection."""
        return secondary_with_tags_server_selector(
            self.tag_sets,
            max_staleness_selectors.select(
                self.max_staleness, selection))


class SecondaryPreferred(_ServerMode):
    """SecondaryPreferred read preference.

    * When directly connected to one mongod queries are allowed to standalone
      servers, to a replica set primary, or to replica set secondaries.
    * When connected to a mongos queries are distributed among shard
      secondaries, or the shard primary if no secondary is available.
    * When connected to a replica set queries are distributed among
      secondaries, or the primary if no secondary is available.

    :Parameters:
      - `tag_sets`: The :attr:`~tag_sets` for this read preference.
      - `max_staleness`: (integer, in seconds) The maximum estimated
        length of time a replica set secondary can fall behind the primary in
        replication before it will no longer be selected for operations.
        Default -1, meaning no maximum. If it is set, it must be at least
        90 seconds.
    """

    __slots__ = ()

    def __init__(self, tag_sets=None, max_staleness=-1):
        super(SecondaryPreferred, self).__init__(_SECONDARY_PREFERRED,
                                                 tag_sets,
                                                 max_staleness)

    def __call__(self, selection):
        """Apply this read preference to Selection."""
        secondaries = secondary_with_tags_server_selector(
            self.tag_sets,
            max_staleness_selectors.select(
                self.max_staleness, selection))

        if secondaries:
            return secondaries
        else:
            return selection.primary_selection


class Nearest(_ServerMode):
    """Nearest read preference.

    * When directly connected to one mongod queries are allowed to standalone
      servers, to a replica set primary, or to replica set secondaries.
    * When connected to a mongos queries are distributed among all members of
      a shard.
    * When connected to a replica set queries are distributed among all
      members.

    :Parameters:
      - `tag_sets`: The :attr:`~tag_sets` for this read preference.
      - `max_staleness`: (integer, in seconds) The maximum estimated
        length of time a replica set secondary can fall behind the primary in
        replication before it will no longer be selected for operations.
        Default -1, meaning no maximum. If it is set, it must be at least
        90 seconds.
    """

    __slots__ = ()

    def __init__(self, tag_sets=None, max_staleness=-1):
        super(Nearest, self).__init__(_NEAREST, tag_sets, max_staleness)

    def __call__(self, selection):
        """Apply this read preference to Selection."""
        return member_with_tags_server_selector(
            self.tag_sets,
            max_staleness_selectors.select(
                self.max_staleness, selection))


_ALL_READ_PREFERENCES = (Primary, PrimaryPreferred,
                         Secondary, SecondaryPreferred, Nearest)


def make_read_preference(mode, tag_sets, max_staleness=-1):
    if mode == _PRIMARY:
        if tag_sets not in (None, [{}]):
            raise ConfigurationError("Read preference primary "
                                     "cannot be combined with tags")
        if max_staleness != -1:
            raise ConfigurationError("Read preference primary cannot be "
                                     "combined with maxStalenessSeconds")
        return Primary()
    return _ALL_READ_PREFERENCES[mode](tag_sets, max_staleness)


_MODES = (
    'PRIMARY',
    'PRIMARY_PREFERRED',
    'SECONDARY',
    'SECONDARY_PREFERRED',
    'NEAREST',
)


class ReadPreference(object):
    """An enum that defines the read preference modes supported by PyMongo.

    See :doc:`/examples/high_availability` for code examples.

    A read preference is used in three cases:

    :class:`~pymongo.mongo_client.MongoClient` connected to a single mongod:

    - ``PRIMARY``: Queries are allowed if the server is standalone or a replica
      set primary.
    - All other modes allow queries to standalone servers, to a replica set
      primary, or to replica set secondaries.

    :class:`~pymongo.mongo_client.MongoClient` initialized with the
    ``replicaSet`` option:

    - ``PRIMARY``: Read from the primary. This is the default, and provides the
      strongest consistency. If no primary is available, raise
      :class:`~pymongo.errors.AutoReconnect`.

    - ``PRIMARY_PREFERRED``: Read from the primary if available, or if there is
      none, read from a secondary.

    - ``SECONDARY``: Read from a secondary. If no secondary is available,
      raise :class:`~pymongo.errors.AutoReconnect`.

    - ``SECONDARY_PREFERRED``: Read from a secondary if available, otherwise
      from the primary.

    - ``NEAREST``: Read from any member.

    :class:`~pymongo.mongo_client.MongoClient` connected to a mongos, with a
    sharded cluster of replica sets:

    - ``PRIMARY``: Read from the primary of the shard, or raise
      :class:`~pymongo.errors.OperationFailure` if there is none.
      This is the default.

    - ``PRIMARY_PREFERRED``: Read from the primary of the shard, or if there is
      none, read from a secondary of the shard.

    - ``SECONDARY``: Read from a secondary of the shard, or raise
      :class:`~pymongo.errors.OperationFailure` if there is none.

    - ``SECONDARY_PREFERRED``: Read from a secondary of the shard if available,
      otherwise from the shard primary.

    - ``NEAREST``: Read from any shard member.
    """
    PRIMARY = Primary()
    PRIMARY_PREFERRED = PrimaryPreferred()
    SECONDARY = Secondary()
    SECONDARY_PREFERRED = SecondaryPreferred()
    NEAREST = Nearest()


def read_pref_mode_from_name(name):
    """Get the read preference mode from mongos/uri name.
    """
    return _MONGOS_MODES.index(name)


class MovingAverage(object):
    """Tracks an exponentially-weighted moving average."""
    def __init__(self):
        self.average = None

    def add_sample(self, sample):
        if sample < 0:
            # Likely system time change while waiting for ismaster response
            # and not using time.monotonic. Ignore it, the next one will
            # probably be valid.
            return
        if self.average is None:
            self.average = sample
        else:
            # The Server Selection Spec requires an exponentially weighted
            # average with alpha = 0.2.
            self.average = 0.8 * self.average + 0.2 * sample

    def get(self):
        """Get the calculated average, or None if no samples yet."""
        return self.average

    def reset(self):
        self.average = None
