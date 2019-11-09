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

"""Tools for working with write concerns."""

from bson.py3compat import integer_types, string_type
from pymongo.errors import ConfigurationError


class WriteConcern(object):
    """WriteConcern

    :Parameters:
        - `w`: (integer or string) Used with replication, write operations
          will block until they have been replicated to the specified number
          or tagged set of servers. `w=<integer>` always includes the replica
          set primary (e.g. w=3 means write to the primary and wait until
          replicated to **two** secondaries). **w=0 disables acknowledgement
          of write operations and can not be used with other write concern
          options.**
        - `wtimeout`: (integer) Used in conjunction with `w`. Specify a value
          in milliseconds to control how long to wait for write propagation
          to complete. If replication does not complete in the given
          timeframe, a timeout exception is raised.
        - `j`: If ``True`` block until write operations have been committed
          to the journal. Cannot be used in combination with `fsync`. Prior
          to MongoDB 2.6 this option was ignored if the server was running
          without journaling. Starting with MongoDB 2.6 write operations will
          fail with an exception if this option is used when the server is
          running without journaling.
        - `fsync`: If ``True`` and the server is running without journaling,
          blocks until the server has synced all data files to disk. If the
          server is running with journaling, this acts the same as the `j`
          option, blocking until write operations have been committed to the
          journal. Cannot be used in combination with `j`.
    """

    __slots__ = ("__document", "__acknowledged", "__server_default")

    def __init__(self, w=None, wtimeout=None, j=None, fsync=None):
        self.__document = {}
        self.__acknowledged = True

        if wtimeout is not None:
            if not isinstance(wtimeout, integer_types):
                raise TypeError("wtimeout must be an integer")
            if wtimeout < 0:
                raise ValueError("wtimeout cannot be less than 0")
            self.__document["wtimeout"] = wtimeout

        if j is not None:
            if not isinstance(j, bool):
                raise TypeError("j must be True or False")
            self.__document["j"] = j

        if fsync is not None:
            if not isinstance(fsync, bool):
                raise TypeError("fsync must be True or False")
            if j and fsync:
                raise ConfigurationError("Can't set both j "
                                         "and fsync at the same time")
            self.__document["fsync"] = fsync

        if w == 0 and j is True:
            raise ConfigurationError("Cannot set w to 0 and j to True")

        if w is not None:
            if isinstance(w, integer_types):
                if w < 0:
                    raise ValueError("w cannot be less than 0")
                self.__acknowledged = w > 0
            elif not isinstance(w, string_type):
                raise TypeError("w must be an integer or string")
            self.__document["w"] = w

        self.__server_default = not self.__document

    @property
    def is_server_default(self):
        """Does this WriteConcern match the server default."""
        return self.__server_default

    @property
    def document(self):
        """The document representation of this write concern.

        .. note::
          :class:`WriteConcern` is immutable. Mutating the value of
          :attr:`document` does not mutate this :class:`WriteConcern`.
        """
        return self.__document.copy()

    @property
    def acknowledged(self):
        """If ``True`` write operations will wait for acknowledgement before
        returning.
        """
        return self.__acknowledged

    def __repr__(self):
        return ("WriteConcern(%s)" % (
            ", ".join("%s=%s" % kvt for kvt in self.__document.items()),))

    def __eq__(self, other):
        if isinstance(other, WriteConcern):
            return self.__document == other.document
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, WriteConcern):
            return self.__document != other.document
        return NotImplemented


DEFAULT_WRITE_CONCERN = WriteConcern()
