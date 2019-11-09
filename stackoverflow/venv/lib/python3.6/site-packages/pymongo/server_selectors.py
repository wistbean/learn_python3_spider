# Copyright 2014-2016 MongoDB, Inc.
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

"""Criteria to select some ServerDescriptions from a TopologyDescription."""

from pymongo.server_type import SERVER_TYPE


class Selection(object):
    """Input or output of a server selector function."""

    @classmethod
    def from_topology_description(cls, topology_description):
        known_servers = topology_description.known_servers
        primary = None
        for sd in known_servers:
            if sd.server_type == SERVER_TYPE.RSPrimary:
                primary = sd
                break

        return Selection(topology_description,
                         topology_description.known_servers,
                         topology_description.common_wire_version,
                         primary)

    def __init__(self,
                 topology_description,
                 server_descriptions,
                 common_wire_version,
                 primary):
        self.topology_description = topology_description
        self.server_descriptions = server_descriptions
        self.primary = primary
        self.common_wire_version = common_wire_version

    def with_server_descriptions(self, server_descriptions):
        return Selection(self.topology_description,
                         server_descriptions,
                         self.common_wire_version,
                         self.primary)

    def secondary_with_max_last_write_date(self):
        secondaries = secondary_server_selector(self)
        if secondaries.server_descriptions:
            return max(secondaries.server_descriptions,
                       key=lambda sd: sd.last_write_date)

    @property
    def primary_selection(self):
        primaries = [self.primary] if self.primary else []
        return self.with_server_descriptions(primaries)

    @property
    def heartbeat_frequency(self):
        return self.topology_description.heartbeat_frequency

    @property
    def topology_type(self):
        return self.topology_description.topology_type

    def __bool__(self):
        return bool(self.server_descriptions)

    __nonzero__ = __bool__  # Python 2.

    def __getitem__(self, item):
        return self.server_descriptions[item]


def any_server_selector(selection):
    return selection


def readable_server_selector(selection):
    return selection.with_server_descriptions(
        [s for s in selection.server_descriptions if s.is_readable])


def writable_server_selector(selection):
    return selection.with_server_descriptions(
        [s for s in selection.server_descriptions if s.is_writable])


def secondary_server_selector(selection):
    return selection.with_server_descriptions(
        [s for s in selection.server_descriptions
         if s.server_type == SERVER_TYPE.RSSecondary])


def arbiter_server_selector(selection):
    return selection.with_server_descriptions(
        [s for s in selection.server_descriptions
         if s.server_type == SERVER_TYPE.RSArbiter])


def writable_preferred_server_selector(selection):
    """Like PrimaryPreferred but doesn't use tags or latency."""
    return (writable_server_selector(selection) or
            secondary_server_selector(selection))


def apply_single_tag_set(tag_set, selection):
    """All servers matching one tag set.

    A tag set is a dict. A server matches if its tags are a superset:
    A server tagged {'a': '1', 'b': '2'} matches the tag set {'a': '1'}.

    The empty tag set {} matches any server.
    """
    def tags_match(server_tags):
        for key, value in tag_set.items():
            if key not in server_tags or server_tags[key] != value:
                return False

        return True

    return selection.with_server_descriptions(
        [s for s in selection.server_descriptions if tags_match(s.tags)])


def apply_tag_sets(tag_sets, selection):
    """All servers match a list of tag sets.

    tag_sets is a list of dicts. The empty tag set {} matches any server,
    and may be provided at the end of the list as a fallback. So
    [{'a': 'value'}, {}] expresses a preference for servers tagged
    {'a': 'value'}, but accepts any server if none matches the first
    preference.
    """
    for tag_set in tag_sets:
        with_tag_set = apply_single_tag_set(tag_set, selection)
        if with_tag_set:
            return with_tag_set

    return selection.with_server_descriptions([])


def secondary_with_tags_server_selector(tag_sets, selection):
    """All near-enough secondaries matching the tag sets."""
    return apply_tag_sets(tag_sets, secondary_server_selector(selection))


def member_with_tags_server_selector(tag_sets, selection):
    """All near-enough members matching the tag sets."""
    return apply_tag_sets(tag_sets, readable_server_selector(selection))
