# Copyright 2009-present MongoDB, Inc.
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

"""Cursor class to iterate over Mongo query results."""

import copy
import warnings

from collections import deque

from bson import RE_TYPE
from bson.code import Code
from bson.py3compat import (iteritems,
                            integer_types,
                            string_type)
from bson.son import SON
from pymongo import helpers
from pymongo.common import validate_boolean, validate_is_mapping
from pymongo.collation import validate_collation_or_none
from pymongo.errors import (ConnectionFailure,
                            InvalidOperation,
                            NotMasterError,
                            OperationFailure)
from pymongo.message import (_CursorAddress,
                             _GetMore,
                             _RawBatchGetMore,
                             _Query,
                             _RawBatchQuery)
from pymongo.monitoring import ConnectionClosedReason


_QUERY_OPTIONS = {
    "tailable_cursor": 2,
    "slave_okay": 4,
    "oplog_replay": 8,
    "no_timeout": 16,
    "await_data": 32,
    "exhaust": 64,
    "partial": 128}


class CursorType(object):
    NON_TAILABLE = 0
    """The standard cursor type."""

    TAILABLE = _QUERY_OPTIONS["tailable_cursor"]
    """The tailable cursor type.

    Tailable cursors are only for use with capped collections. They are not
    closed when the last data is retrieved but are kept open and the cursor
    location marks the final document position. If more data is received
    iteration of the cursor will continue from the last document received.
    """

    TAILABLE_AWAIT = TAILABLE | _QUERY_OPTIONS["await_data"]
    """A tailable cursor with the await option set.

    Creates a tailable cursor that will wait for a few seconds after returning
    the full result set so that it can capture and return additional data added
    during the query.
    """

    EXHAUST = _QUERY_OPTIONS["exhaust"]
    """An exhaust cursor.

    MongoDB will stream batched results to the client without waiting for the
    client to request each batch, reducing latency.
    """


# This has to be an old style class due to
# http://bugs.jython.org/issue1057
class _SocketManager:
    """Used with exhaust cursors to ensure the socket is returned.
    """
    def __init__(self, sock, pool):
        self.sock = sock
        self.pool = pool
        self.__closed = False

    def __del__(self):
        self.close()

    def close(self):
        """Return this instance's socket to the connection pool.
        """
        if not self.__closed:
            self.__closed = True
            self.pool.return_socket(self.sock)
            self.sock, self.pool = None, None


class Cursor(object):
    """A cursor / iterator over Mongo query results.
    """
    _query_class = _Query
    _getmore_class = _GetMore

    def __init__(self, collection, filter=None, projection=None, skip=0,
                 limit=0, no_cursor_timeout=False,
                 cursor_type=CursorType.NON_TAILABLE,
                 sort=None, allow_partial_results=False, oplog_replay=False,
                 modifiers=None, batch_size=0, manipulate=True,
                 collation=None, hint=None, max_scan=None, max_time_ms=None,
                 max=None, min=None, return_key=False, show_record_id=False,
                 snapshot=False, comment=None, session=None):
        """Create a new cursor.

        Should not be called directly by application developers - see
        :meth:`~pymongo.collection.Collection.find` instead.

        .. mongodoc:: cursors
        """
        # Initialize all attributes used in __del__ before possibly raising
        # an error to avoid attribute errors during garbage collection.
        self.__id = None
        self.__exhaust = False
        self.__exhaust_mgr = None
        self.__killed = False

        if session:
            self.__session = session
            self.__explicit_session = True
        else:
            self.__session = None
            self.__explicit_session = False

        spec = filter
        if spec is None:
            spec = {}

        validate_is_mapping("filter", spec)
        if not isinstance(skip, int):
            raise TypeError("skip must be an instance of int")
        if not isinstance(limit, int):
            raise TypeError("limit must be an instance of int")
        validate_boolean("no_cursor_timeout", no_cursor_timeout)
        if cursor_type not in (CursorType.NON_TAILABLE, CursorType.TAILABLE,
                               CursorType.TAILABLE_AWAIT, CursorType.EXHAUST):
            raise ValueError("not a valid value for cursor_type")
        validate_boolean("allow_partial_results", allow_partial_results)
        validate_boolean("oplog_replay", oplog_replay)
        if modifiers is not None:
            warnings.warn("the 'modifiers' parameter is deprecated",
                          DeprecationWarning, stacklevel=2)
            validate_is_mapping("modifiers", modifiers)
        if not isinstance(batch_size, integer_types):
            raise TypeError("batch_size must be an integer")
        if batch_size < 0:
            raise ValueError("batch_size must be >= 0")

        if projection is not None:
            if not projection:
                projection = {"_id": 1}
            projection = helpers._fields_list_to_dict(projection, "projection")

        self.__collection = collection
        self.__spec = spec
        self.__projection = projection
        self.__skip = skip
        self.__limit = limit
        self.__batch_size = batch_size
        self.__modifiers = modifiers and modifiers.copy() or {}
        self.__ordering = sort and helpers._index_document(sort) or None
        self.__max_scan = max_scan
        self.__explain = False
        self.__comment = comment
        self.__max_time_ms = max_time_ms
        self.__max_await_time_ms = None
        self.__max = max
        self.__min = min
        self.__manipulate = manipulate
        self.__collation = validate_collation_or_none(collation)
        self.__return_key = return_key
        self.__show_record_id = show_record_id
        self.__snapshot = snapshot
        self.__set_hint(hint)

        # Exhaust cursor support
        if cursor_type == CursorType.EXHAUST:
            if self.__collection.database.client.is_mongos:
                raise InvalidOperation('Exhaust cursors are '
                                       'not supported by mongos')
            if limit:
                raise InvalidOperation("Can't use limit and exhaust together.")
            self.__exhaust = True

        # This is ugly. People want to be able to do cursor[5:5] and
        # get an empty result set (old behavior was an
        # exception). It's hard to do that right, though, because the
        # server uses limit(0) to mean 'no limit'. So we set __empty
        # in that case and check for it when iterating. We also unset
        # it anytime we change __limit.
        self.__empty = False

        self.__data = deque()
        self.__address = None
        self.__retrieved = 0

        self.__codec_options = collection.codec_options
        # Read preference is set when the initial find is sent.
        self.__read_preference = None
        self.__read_concern = collection.read_concern

        self.__query_flags = cursor_type
        if no_cursor_timeout:
            self.__query_flags |= _QUERY_OPTIONS["no_timeout"]
        if allow_partial_results:
            self.__query_flags |= _QUERY_OPTIONS["partial"]
        if oplog_replay:
            self.__query_flags |= _QUERY_OPTIONS["oplog_replay"]

    @property
    def collection(self):
        """The :class:`~pymongo.collection.Collection` that this
        :class:`Cursor` is iterating.
        """
        return self.__collection

    @property
    def retrieved(self):
        """The number of documents retrieved so far.
        """
        return self.__retrieved

    def __del__(self):
        self.__die()

    def rewind(self):
        """Rewind this cursor to its unevaluated state.

        Reset this cursor if it has been partially or completely evaluated.
        Any options that are present on the cursor will remain in effect.
        Future iterating performed on this cursor will cause new queries to
        be sent to the server, even if the resultant data has already been
        retrieved by this cursor.
        """
        self.__data = deque()
        self.__id = None
        self.__address = None
        self.__retrieved = 0
        self.__killed = False

        return self

    def clone(self):
        """Get a clone of this cursor.

        Returns a new Cursor instance with options matching those that have
        been set on the current instance. The clone will be completely
        unevaluated, even if the current instance has been partially or
        completely evaluated.
        """
        return self._clone(True)

    def _clone(self, deepcopy=True, base=None):
        """Internal clone helper."""
        if not base:
            if self.__explicit_session:
                base = self._clone_base(self.__session)
            else:
                base = self._clone_base(None)

        values_to_clone = ("spec", "projection", "skip", "limit",
                           "max_time_ms", "max_await_time_ms", "comment",
                           "max", "min", "ordering", "explain", "hint",
                           "batch_size", "max_scan", "manipulate",
                           "query_flags", "modifiers", "collation")
        data = dict((k, v) for k, v in iteritems(self.__dict__)
                    if k.startswith('_Cursor__') and k[9:] in values_to_clone)
        if deepcopy:
            data = self._deepcopy(data)
        base.__dict__.update(data)
        return base

    def _clone_base(self, session):
        """Creates an empty Cursor object for information to be copied into.
        """
        return self.__class__(self.__collection, session=session)

    def __die(self, synchronous=False):
        """Closes this cursor.
        """
        try:
            already_killed = self.__killed
        except AttributeError:
            # __init__ did not run to completion (or at all).
            return

        self.__killed = True
        if self.__id and not already_killed:
            if self.__exhaust and self.__exhaust_mgr:
                # If this is an exhaust cursor and we haven't completely
                # exhausted the result set we *must* close the socket
                # to stop the server from sending more data.
                self.__exhaust_mgr.sock.close_socket(
                    ConnectionClosedReason.ERROR)
            else:
                address = _CursorAddress(
                    self.__address, self.__collection.full_name)
                if synchronous:
                    self.__collection.database.client._close_cursor_now(
                        self.__id, address, session=self.__session)
                else:
                    # The cursor will be closed later in a different session.
                    self.__collection.database.client._close_cursor(
                        self.__id, address)
        if self.__exhaust and self.__exhaust_mgr:
            self.__exhaust_mgr.close()
        if self.__session and not self.__explicit_session:
            self.__session._end_session(lock=synchronous)
            self.__session = None

    def close(self):
        """Explicitly close / kill this cursor.
        """
        self.__die(True)

    def __query_spec(self):
        """Get the spec to use for a query.
        """
        operators = self.__modifiers.copy()
        if self.__ordering:
            operators["$orderby"] = self.__ordering
        if self.__explain:
            operators["$explain"] = True
        if self.__hint:
            operators["$hint"] = self.__hint
        if self.__comment:
            operators["$comment"] = self.__comment
        if self.__max_scan:
            operators["$maxScan"] = self.__max_scan
        if self.__max_time_ms is not None:
            operators["$maxTimeMS"] = self.__max_time_ms
        if self.__max:
            operators["$max"] = self.__max
        if self.__min:
            operators["$min"] = self.__min
        if self.__return_key:
            operators["$returnKey"] = self.__return_key
        if self.__show_record_id:
            # This is upgraded to showRecordId for MongoDB 3.2+ "find" command.
            operators["$showDiskLoc"] = self.__show_record_id
        if self.__snapshot:
            operators["$snapshot"] = self.__snapshot

        if operators:
            # Make a shallow copy so we can cleanly rewind or clone.
            spec = self.__spec.copy()

            # White-listed commands must be wrapped in $query.
            if "$query" not in spec:
                # $query has to come first
                spec = SON([("$query", spec)])

            if not isinstance(spec, SON):
                # Ensure the spec is SON. As order is important this will
                # ensure its set before merging in any extra operators.
                spec = SON(spec)

            spec.update(operators)
            return spec
        # Have to wrap with $query if "query" is the first key.
        # We can't just use $query anytime "query" is a key as
        # that breaks commands like count and find_and_modify.
        # Checking spec.keys()[0] covers the case that the spec
        # was passed as an instance of SON or OrderedDict.
        elif ("query" in self.__spec and
              (len(self.__spec) == 1 or
               next(iter(self.__spec)) == "query")):
            return SON({"$query": self.__spec})

        return self.__spec

    def __check_okay_to_chain(self):
        """Check if it is okay to chain more options onto this cursor.
        """
        if self.__retrieved or self.__id is not None:
            raise InvalidOperation("cannot set options after executing query")

    def add_option(self, mask):
        """Set arbitrary query flags using a bitmask.

        To set the tailable flag:
        cursor.add_option(2)
        """
        if not isinstance(mask, int):
            raise TypeError("mask must be an int")
        self.__check_okay_to_chain()

        if mask & _QUERY_OPTIONS["exhaust"]:
            if self.__limit:
                raise InvalidOperation("Can't use limit and exhaust together.")
            if self.__collection.database.client.is_mongos:
                raise InvalidOperation('Exhaust cursors are '
                                       'not supported by mongos')
            self.__exhaust = True

        self.__query_flags |= mask
        return self

    def remove_option(self, mask):
        """Unset arbitrary query flags using a bitmask.

        To unset the tailable flag:
        cursor.remove_option(2)
        """
        if not isinstance(mask, int):
            raise TypeError("mask must be an int")
        self.__check_okay_to_chain()

        if mask & _QUERY_OPTIONS["exhaust"]:
            self.__exhaust = False

        self.__query_flags &= ~mask
        return self

    def limit(self, limit):
        """Limits the number of results to be returned by this cursor.

        Raises :exc:`TypeError` if `limit` is not an integer. Raises
        :exc:`~pymongo.errors.InvalidOperation` if this :class:`Cursor`
        has already been used. The last `limit` applied to this cursor
        takes precedence. A limit of ``0`` is equivalent to no limit.

        :Parameters:
          - `limit`: the number of results to return

        .. mongodoc:: limit
        """
        if not isinstance(limit, integer_types):
            raise TypeError("limit must be an integer")
        if self.__exhaust:
            raise InvalidOperation("Can't use limit and exhaust together.")
        self.__check_okay_to_chain()

        self.__empty = False
        self.__limit = limit
        return self

    def batch_size(self, batch_size):
        """Limits the number of documents returned in one batch. Each batch
        requires a round trip to the server. It can be adjusted to optimize
        performance and limit data transfer.

        .. note:: batch_size can not override MongoDB's internal limits on the
           amount of data it will return to the client in a single batch (i.e
           if you set batch size to 1,000,000,000, MongoDB will currently only
           return 4-16MB of results per batch).

        Raises :exc:`TypeError` if `batch_size` is not an integer.
        Raises :exc:`ValueError` if `batch_size` is less than ``0``.
        Raises :exc:`~pymongo.errors.InvalidOperation` if this
        :class:`Cursor` has already been used. The last `batch_size`
        applied to this cursor takes precedence.

        :Parameters:
          - `batch_size`: The size of each batch of results requested.
        """
        if not isinstance(batch_size, integer_types):
            raise TypeError("batch_size must be an integer")
        if batch_size < 0:
            raise ValueError("batch_size must be >= 0")
        self.__check_okay_to_chain()

        self.__batch_size = batch_size
        return self

    def skip(self, skip):
        """Skips the first `skip` results of this cursor.

        Raises :exc:`TypeError` if `skip` is not an integer. Raises
        :exc:`ValueError` if `skip` is less than ``0``. Raises
        :exc:`~pymongo.errors.InvalidOperation` if this :class:`Cursor` has
        already been used. The last `skip` applied to this cursor takes
        precedence.

        :Parameters:
          - `skip`: the number of results to skip
        """
        if not isinstance(skip, integer_types):
            raise TypeError("skip must be an integer")
        if skip < 0:
            raise ValueError("skip must be >= 0")
        self.__check_okay_to_chain()

        self.__skip = skip
        return self

    def max_time_ms(self, max_time_ms):
        """Specifies a time limit for a query operation. If the specified
        time is exceeded, the operation will be aborted and
        :exc:`~pymongo.errors.ExecutionTimeout` is raised. If `max_time_ms`
        is ``None`` no limit is applied.

        Raises :exc:`TypeError` if `max_time_ms` is not an integer or ``None``.
        Raises :exc:`~pymongo.errors.InvalidOperation` if this :class:`Cursor`
        has already been used.

        :Parameters:
          - `max_time_ms`: the time limit after which the operation is aborted
        """
        if (not isinstance(max_time_ms, integer_types)
                and max_time_ms is not None):
            raise TypeError("max_time_ms must be an integer or None")
        self.__check_okay_to_chain()

        self.__max_time_ms = max_time_ms
        return self

    def max_await_time_ms(self, max_await_time_ms):
        """Specifies a time limit for a getMore operation on a
        :attr:`~pymongo.cursor.CursorType.TAILABLE_AWAIT` cursor. For all other
        types of cursor max_await_time_ms is ignored.

        Raises :exc:`TypeError` if `max_await_time_ms` is not an integer or
        ``None``. Raises :exc:`~pymongo.errors.InvalidOperation` if this
        :class:`Cursor` has already been used.

        .. note:: `max_await_time_ms` requires server version **>= 3.2**

        :Parameters:
          - `max_await_time_ms`: the time limit after which the operation is
            aborted

        .. versionadded:: 3.2
        """
        if (not isinstance(max_await_time_ms, integer_types)
                and max_await_time_ms is not None):
            raise TypeError("max_await_time_ms must be an integer or None")
        self.__check_okay_to_chain()

        # Ignore max_await_time_ms if not tailable or await_data is False.
        if self.__query_flags & CursorType.TAILABLE_AWAIT:
            self.__max_await_time_ms = max_await_time_ms

        return self

    def __getitem__(self, index):
        """Get a single document or a slice of documents from this cursor.

        Raises :class:`~pymongo.errors.InvalidOperation` if this
        cursor has already been used.

        To get a single document use an integral index, e.g.::

          >>> db.test.find()[50]

        An :class:`IndexError` will be raised if the index is negative
        or greater than the amount of documents in this cursor. Any
        limit previously applied to this cursor will be ignored.

        To get a slice of documents use a slice index, e.g.::

          >>> db.test.find()[20:25]

        This will return this cursor with a limit of ``5`` and skip of
        ``20`` applied.  Using a slice index will override any prior
        limits or skips applied to this cursor (including those
        applied through previous calls to this method). Raises
        :class:`IndexError` when the slice has a step, a negative
        start value, or a stop value less than or equal to the start
        value.

        :Parameters:
          - `index`: An integer or slice index to be applied to this cursor
        """
        self.__check_okay_to_chain()
        self.__empty = False
        if isinstance(index, slice):
            if index.step is not None:
                raise IndexError("Cursor instances do not support slice steps")

            skip = 0
            if index.start is not None:
                if index.start < 0:
                    raise IndexError("Cursor instances do not support "
                                     "negative indices")
                skip = index.start

            if index.stop is not None:
                limit = index.stop - skip
                if limit < 0:
                    raise IndexError("stop index must be greater than start "
                                     "index for slice %r" % index)
                if limit == 0:
                    self.__empty = True
            else:
                limit = 0

            self.__skip = skip
            self.__limit = limit
            return self

        if isinstance(index, integer_types):
            if index < 0:
                raise IndexError("Cursor instances do not support negative "
                                 "indices")
            clone = self.clone()
            clone.skip(index + self.__skip)
            clone.limit(-1)  # use a hard limit
            clone.__query_flags &= ~CursorType.TAILABLE_AWAIT  # PYTHON-1371
            for doc in clone:
                return doc
            raise IndexError("no such item for Cursor instance")
        raise TypeError("index %r cannot be applied to Cursor "
                        "instances" % index)

    def max_scan(self, max_scan):
        """**DEPRECATED** - Limit the number of documents to scan when
        performing the query.

        Raises :class:`~pymongo.errors.InvalidOperation` if this
        cursor has already been used. Only the last :meth:`max_scan`
        applied to this cursor has any effect.

        :Parameters:
          - `max_scan`: the maximum number of documents to scan

        .. versionchanged:: 3.7
          Deprecated :meth:`max_scan`. Support for this option is deprecated in
          MongoDB 4.0. Use :meth:`max_time_ms` instead to limit server side
          execution time.
        """
        self.__check_okay_to_chain()
        self.__max_scan = max_scan
        return self

    def max(self, spec):
        """Adds ``max`` operator that specifies upper bound for specific index.

        When using ``max``, :meth:`~hint` should also be configured to ensure
        the query uses the expected index and starting in MongoDB 4.2
        :meth:`~hint` will be required.

        :Parameters:
          - `spec`: a list of field, limit pairs specifying the exclusive
            upper bound for all keys of a specific index in order.

        .. versionchanged:: 3.8
           Deprecated cursors that use ``max`` without a :meth:`~hint`.

        .. versionadded:: 2.7
        """
        if not isinstance(spec, (list, tuple)):
            raise TypeError("spec must be an instance of list or tuple")

        self.__check_okay_to_chain()
        self.__max = SON(spec)
        return self

    def min(self, spec):
        """Adds ``min`` operator that specifies lower bound for specific index.

        When using ``min``, :meth:`~hint` should also be configured to ensure
        the query uses the expected index and starting in MongoDB 4.2
        :meth:`~hint` will be required.

        :Parameters:
          - `spec`: a list of field, limit pairs specifying the inclusive
            lower bound for all keys of a specific index in order.

        .. versionchanged:: 3.8
           Deprecated cursors that use ``min`` without a :meth:`~hint`.

        .. versionadded:: 2.7
        """
        if not isinstance(spec, (list, tuple)):
            raise TypeError("spec must be an instance of list or tuple")

        self.__check_okay_to_chain()
        self.__min = SON(spec)
        return self

    def sort(self, key_or_list, direction=None):
        """Sorts this cursor's results.

        Pass a field name and a direction, either
        :data:`~pymongo.ASCENDING` or :data:`~pymongo.DESCENDING`::

            for doc in collection.find().sort('field', pymongo.ASCENDING):
                print(doc)

        To sort by multiple fields, pass a list of (key, direction) pairs::

            for doc in collection.find().sort([
                    ('field1', pymongo.ASCENDING),
                    ('field2', pymongo.DESCENDING)]):
                print(doc)

        Beginning with MongoDB version 2.6, text search results can be
        sorted by relevance::

            cursor = db.test.find(
                {'$text': {'$search': 'some words'}},
                {'score': {'$meta': 'textScore'}})

            # Sort by 'score' field.
            cursor.sort([('score', {'$meta': 'textScore'})])

            for doc in cursor:
                print(doc)

        Raises :class:`~pymongo.errors.InvalidOperation` if this cursor has
        already been used. Only the last :meth:`sort` applied to this
        cursor has any effect.

        :Parameters:
          - `key_or_list`: a single key or a list of (key, direction)
            pairs specifying the keys to sort on
          - `direction` (optional): only used if `key_or_list` is a single
            key, if not given :data:`~pymongo.ASCENDING` is assumed
        """
        self.__check_okay_to_chain()
        keys = helpers._index_list(key_or_list, direction)
        self.__ordering = helpers._index_document(keys)
        return self

    def count(self, with_limit_and_skip=False):
        """**DEPRECATED** - Get the size of the results set for this query.

        The :meth:`count` method is deprecated and **not** supported in a
        transaction. Please use
        :meth:`~pymongo.collection.Collection.count_documents` instead.

        Returns the number of documents in the results set for this query. Does
        not take :meth:`limit` and :meth:`skip` into account by default - set
        `with_limit_and_skip` to ``True`` if that is the desired behavior.
        Raises :class:`~pymongo.errors.OperationFailure` on a database error.

        When used with MongoDB >= 2.6, :meth:`~count` uses any :meth:`~hint`
        applied to the query. In the following example the hint is passed to
        the count command:

          collection.find({'field': 'value'}).hint('field_1').count()

        The :meth:`count` method obeys the
        :attr:`~pymongo.collection.Collection.read_preference` of the
        :class:`~pymongo.collection.Collection` instance on which
        :meth:`~pymongo.collection.Collection.find` was called.

        :Parameters:
          - `with_limit_and_skip` (optional): take any :meth:`limit` or
            :meth:`skip` that has been applied to this cursor into account when
            getting the count

        .. note:: The `with_limit_and_skip` parameter requires server
           version **>= 1.1.4-**

        .. versionchanged:: 3.7
           Deprecated.

        .. versionchanged:: 2.8
           The :meth:`~count` method now supports :meth:`~hint`.
        """
        warnings.warn("count is deprecated. Use Collection.count_documents "
                      "instead.", DeprecationWarning, stacklevel=2)
        validate_boolean("with_limit_and_skip", with_limit_and_skip)
        cmd = SON([("count", self.__collection.name),
                   ("query", self.__spec)])
        if self.__max_time_ms is not None:
            cmd["maxTimeMS"] = self.__max_time_ms
        if self.__comment:
            cmd["comment"] = self.__comment

        if self.__hint is not None:
            cmd["hint"] = self.__hint

        if with_limit_and_skip:
            if self.__limit:
                cmd["limit"] = self.__limit
            if self.__skip:
                cmd["skip"] = self.__skip

        return self.__collection._count(
            cmd, self.__collation, session=self.__session)

    def distinct(self, key):
        """Get a list of distinct values for `key` among all documents
        in the result set of this query.

        Raises :class:`TypeError` if `key` is not an instance of
        :class:`basestring` (:class:`str` in python 3).

        The :meth:`distinct` method obeys the
        :attr:`~pymongo.collection.Collection.read_preference` of the
        :class:`~pymongo.collection.Collection` instance on which
        :meth:`~pymongo.collection.Collection.find` was called.

        :Parameters:
          - `key`: name of key for which we want to get the distinct values

        .. seealso:: :meth:`pymongo.collection.Collection.distinct`
        """
        options = {}
        if self.__spec:
            options["query"] = self.__spec
        if self.__max_time_ms is not None:
            options['maxTimeMS'] = self.__max_time_ms
        if self.__comment:
            options['comment'] = self.__comment
        if self.__collation is not None:
            options['collation'] = self.__collation

        return self.__collection.distinct(
            key, session=self.__session, **options)

    def explain(self):
        """Returns an explain plan record for this cursor.

        .. note:: Starting with MongoDB 3.2 :meth:`explain` uses
          the default verbosity mode of the `explain command
          <https://docs.mongodb.com/manual/reference/command/explain/>`_,
          ``allPlansExecution``. To use a different verbosity use
          :meth:`~pymongo.database.Database.command` to run the explain
          command directly.

        .. mongodoc:: explain
        """
        c = self.clone()
        c.__explain = True

        # always use a hard limit for explains
        if c.__limit:
            c.__limit = -abs(c.__limit)
        return next(c)

    def __set_hint(self, index):
        if index is None:
            self.__hint = None
            return

        if isinstance(index, string_type):
            self.__hint = index
        else:
            self.__hint = helpers._index_document(index)

    def hint(self, index):
        """Adds a 'hint', telling Mongo the proper index to use for the query.

        Judicious use of hints can greatly improve query
        performance. When doing a query on multiple fields (at least
        one of which is indexed) pass the indexed field as a hint to
        the query. Raises :class:`~pymongo.errors.OperationFailure` if the
        provided hint requires an index that does not exist on this collection,
        and raises :class:`~pymongo.errors.InvalidOperation` if this cursor has
        already been used.

        `index` should be an index as passed to
        :meth:`~pymongo.collection.Collection.create_index`
        (e.g. ``[('field', ASCENDING)]``) or the name of the index.
        If `index` is ``None`` any existing hint for this query is
        cleared. The last hint applied to this cursor takes precedence
        over all others.

        :Parameters:
          - `index`: index to hint on (as an index specifier)

        .. versionchanged:: 2.8
           The :meth:`~hint` method accepts the name of the index.
        """
        self.__check_okay_to_chain()
        self.__set_hint(index)
        return self

    def comment(self, comment):
        """Adds a 'comment' to the cursor.

        http://docs.mongodb.org/manual/reference/operator/comment/

        :Parameters:
          - `comment`: A string to attach to the query to help interpret and
            trace the operation in the server logs and in profile data.

        .. versionadded:: 2.7
        """
        self.__check_okay_to_chain()
        self.__comment = comment
        return self

    def where(self, code):
        """Adds a $where clause to this query.

        The `code` argument must be an instance of :class:`basestring`
        (:class:`str` in python 3) or :class:`~bson.code.Code`
        containing a JavaScript expression. This expression will be
        evaluated for each document scanned. Only those documents
        for which the expression evaluates to *true* will be returned
        as results. The keyword *this* refers to the object currently
        being scanned.

        Raises :class:`TypeError` if `code` is not an instance of
        :class:`basestring` (:class:`str` in python 3). Raises
        :class:`~pymongo.errors.InvalidOperation` if this
        :class:`Cursor` has already been used. Only the last call to
        :meth:`where` applied to a :class:`Cursor` has any effect.

        :Parameters:
          - `code`: JavaScript expression to use as a filter
        """
        self.__check_okay_to_chain()
        if not isinstance(code, Code):
            code = Code(code)

        self.__spec["$where"] = code
        return self

    def collation(self, collation):
        """Adds a :class:`~pymongo.collation.Collation` to this query.

        This option is only supported on MongoDB 3.4 and above.

        Raises :exc:`TypeError` if `collation` is not an instance of
        :class:`~pymongo.collation.Collation` or a ``dict``. Raises
        :exc:`~pymongo.errors.InvalidOperation` if this :class:`Cursor` has
        already been used. Only the last collation applied to this cursor has
        any effect.

        :Parameters:
          - `collation`: An instance of :class:`~pymongo.collation.Collation`.
        """
        self.__check_okay_to_chain()
        self.__collation = validate_collation_or_none(collation)
        return self

    def __send_message(self, operation):
        """Send a query or getmore operation and handles the response.

        If operation is ``None`` this is an exhaust cursor, which reads
        the next result batch off the exhaust socket instead of
        sending getMore messages to the server.

        Can raise ConnectionFailure.
        """
        client = self.__collection.database.client
        # OP_MSG is required to support exhaust cursors with encryption.
        if client._encrypter and self.__exhaust:
            raise InvalidOperation(
                "exhaust cursors do not support auto encryption")

        try:
            response = client._run_operation_with_response(
                operation, self._unpack_response, exhaust=self.__exhaust,
                address=self.__address)
        except OperationFailure:
            self.__killed = True

            # Make sure exhaust socket is returned immediately, if necessary.
            self.__die()

            # If this is a tailable cursor the error is likely
            # due to capped collection roll over. Setting
            # self.__killed to True ensures Cursor.alive will be
            # False. No need to re-raise.
            if self.__query_flags & _QUERY_OPTIONS["tailable_cursor"]:
                return
            raise
        except NotMasterError:
            # Don't send kill cursors to another server after a "not master"
            # error. It's completely pointless.
            self.__killed = True

            # Make sure exhaust socket is returned immediately, if necessary.
            self.__die()

            raise
        except ConnectionFailure:
            # Don't try to send kill cursors on another socket
            # or to another server. It can cause a _pinValue
            # assertion on some server releases if we get here
            # due to a socket timeout.
            self.__killed = True
            self.__die()
            raise
        except Exception:
            # Close the cursor
            self.__die()
            raise

        self.__address = response.address
        if self.__exhaust and not self.__exhaust_mgr:
            # 'response' is an ExhaustResponse.
            self.__exhaust_mgr = _SocketManager(response.socket_info,
                                                response.pool)

        cmd_name = operation.name
        docs = response.docs
        if response.from_command:
            if cmd_name != "explain":
                cursor = docs[0]['cursor']
                self.__id = cursor['id']
                if cmd_name == 'find':
                    documents = cursor['firstBatch']
                else:
                    documents = cursor['nextBatch']
                self.__data = deque(documents)
                self.__retrieved += len(documents)
            else:
                self.__id = 0
                self.__data = deque(docs)
                self.__retrieved += len(docs)
        else:
            self.__id = response.data.cursor_id
            self.__data = deque(docs)
            self.__retrieved += response.data.number_returned

        if self.__id == 0:
            self.__killed = True
            # Don't wait for garbage collection to call __del__, return the
            # socket and the session to the pool now.
            self.__die()

        if self.__limit and self.__id and self.__limit <= self.__retrieved:
            self.__die()

    def _unpack_response(self, response, cursor_id, codec_options,
                         user_fields=None, legacy_response=False):
        return response.unpack_response(cursor_id, codec_options, user_fields,
                                        legacy_response)

    def _read_preference(self):
        if self.__read_preference is None:
            # Save the read preference for getMore commands.
            self.__read_preference = self.__collection._read_preference_for(
                self.session)
        return self.__read_preference

    def _refresh(self):
        """Refreshes the cursor with more data from Mongo.

        Returns the length of self.__data after refresh. Will exit early if
        self.__data is already non-empty. Raises OperationFailure when the
        cursor cannot be refreshed due to an error on the query.
        """
        if len(self.__data) or self.__killed:
            return len(self.__data)

        if not self.__session:
            self.__session = self.__collection.database.client._ensure_session()

        if self.__id is None:  # Query
            if (self.__min or self.__max) and not self.__hint:
                warnings.warn("using a min/max query operator without "
                              "specifying a Cursor.hint is deprecated. A "
                              "hint will be required when using min/max in "
                              "PyMongo 4.0",
                              DeprecationWarning, stacklevel=3)
            q = self._query_class(self.__query_flags,
                                  self.__collection.database.name,
                                  self.__collection.name,
                                  self.__skip,
                                  self.__query_spec(),
                                  self.__projection,
                                  self.__codec_options,
                                  self._read_preference(),
                                  self.__limit,
                                  self.__batch_size,
                                  self.__read_concern,
                                  self.__collation,
                                  self.__session,
                                  self.__collection.database.client)
            self.__send_message(q)
        elif self.__id:  # Get More
            if self.__limit:
                limit = self.__limit - self.__retrieved
                if self.__batch_size:
                    limit = min(limit, self.__batch_size)
            else:
                limit = self.__batch_size

            # Exhaust cursors don't send getMore messages.
            g = self._getmore_class(self.__collection.database.name,
                                    self.__collection.name,
                                    limit,
                                    self.__id,
                                    self.__codec_options,
                                    self._read_preference(),
                                    self.__session,
                                    self.__collection.database.client,
                                    self.__max_await_time_ms,
                                    self.__exhaust_mgr)
            self.__send_message(g)

        return len(self.__data)

    @property
    def alive(self):
        """Does this cursor have the potential to return more data?

        This is mostly useful with `tailable cursors
        <http://www.mongodb.org/display/DOCS/Tailable+Cursors>`_
        since they will stop iterating even though they *may* return more
        results in the future.

        With regular cursors, simply use a for loop instead of :attr:`alive`::

            for doc in collection.find():
                print(doc)

        .. note:: Even if :attr:`alive` is True, :meth:`next` can raise
          :exc:`StopIteration`. :attr:`alive` can also be True while iterating
          a cursor from a failed server. In this case :attr:`alive` will
          return False after :meth:`next` fails to retrieve the next batch
          of results from the server.
        """
        return bool(len(self.__data) or (not self.__killed))

    @property
    def cursor_id(self):
        """Returns the id of the cursor

        Useful if you need to manage cursor ids and want to handle killing
        cursors manually using
        :meth:`~pymongo.mongo_client.MongoClient.kill_cursors`

        .. versionadded:: 2.2
        """
        return self.__id

    @property
    def address(self):
        """The (host, port) of the server used, or None.

        .. versionchanged:: 3.0
           Renamed from "conn_id".
        """
        return self.__address

    @property
    def session(self):
        """The cursor's :class:`~pymongo.client_session.ClientSession`, or None.

        .. versionadded:: 3.6
        """
        if self.__explicit_session:
            return self.__session

    def __iter__(self):
        return self

    def next(self):
        """Advance the cursor."""
        if self.__empty:
            raise StopIteration
        if len(self.__data) or self._refresh():
            if self.__manipulate:
                _db = self.__collection.database
                return _db._fix_outgoing(self.__data.popleft(),
                                         self.__collection)
            else:
                return self.__data.popleft()
        else:
            raise StopIteration

    __next__ = next

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __copy__(self):
        """Support function for `copy.copy()`.

        .. versionadded:: 2.4
        """
        return self._clone(deepcopy=False)

    def __deepcopy__(self, memo):
        """Support function for `copy.deepcopy()`.

        .. versionadded:: 2.4
        """
        return self._clone(deepcopy=True)

    def _deepcopy(self, x, memo=None):
        """Deepcopy helper for the data dictionary or list.

        Regular expressions cannot be deep copied but as they are immutable we
        don't have to copy them when cloning.
        """
        if not hasattr(x, 'items'):
            y, is_list, iterator = [], True, enumerate(x)
        else:
            y, is_list, iterator = {}, False, iteritems(x)

        if memo is None:
            memo = {}
        val_id = id(x)
        if val_id in memo:
            return memo.get(val_id)
        memo[val_id] = y

        for key, value in iterator:
            if isinstance(value, (dict, list)) and not isinstance(value, SON):
                value = self._deepcopy(value, memo)
            elif not isinstance(value, RE_TYPE):
                value = copy.deepcopy(value, memo)

            if is_list:
                y.append(value)
            else:
                if not isinstance(key, RE_TYPE):
                    key = copy.deepcopy(key, memo)
                y[key] = value
        return y


class RawBatchCursor(Cursor):
    """A cursor / iterator over raw batches of BSON data from a query result."""

    _query_class = _RawBatchQuery
    _getmore_class = _RawBatchGetMore

    def __init__(self, *args, **kwargs):
        """Create a new cursor / iterator over raw batches of BSON data.

        Should not be called directly by application developers -
        see :meth:`~pymongo.collection.Collection.find_raw_batches`
        instead.

        .. mongodoc:: cursors
        """
        manipulate = kwargs.get('manipulate')
        kwargs['manipulate'] = False
        super(RawBatchCursor, self).__init__(*args, **kwargs)

        # Throw only after cursor's initialized, to prevent errors in __del__.
        if manipulate:
            raise InvalidOperation(
                "Cannot use RawBatchCursor with manipulate=True")

    def _unpack_response(self, response, cursor_id, codec_options,
                         user_fields=None, legacy_response=False):
        return response.raw_response(cursor_id)

    def explain(self):
        """Returns an explain plan record for this cursor.

        .. mongodoc:: explain
        """
        clone = self._clone(deepcopy=True, base=Cursor(self.collection))
        return clone.explain()

    def __getitem__(self, index):
        raise InvalidOperation("Cannot call __getitem__ on RawBatchCursor")
