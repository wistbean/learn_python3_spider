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

"""CommandCursor class to iterate over command results."""

from collections import deque

from bson.py3compat import integer_types
from pymongo.errors import (ConnectionFailure,
                            InvalidOperation,
                            NotMasterError,
                            OperationFailure)
from pymongo.message import (_CursorAddress,
                             _GetMore,
                             _RawBatchGetMore)


class CommandCursor(object):
    """A cursor / iterator over command cursors."""
    _getmore_class = _GetMore

    def __init__(self, collection, cursor_info, address, retrieved=0,
                 batch_size=0, max_await_time_ms=None, session=None,
                 explicit_session=False):
        """Create a new command cursor.

        The parameter 'retrieved' is unused.
        """
        self.__collection = collection
        self.__id = cursor_info['id']
        self.__data = deque(cursor_info['firstBatch'])
        self.__postbatchresumetoken = cursor_info.get('postBatchResumeToken')
        self.__address = address
        self.__batch_size = batch_size
        self.__max_await_time_ms = max_await_time_ms
        self.__session = session
        self.__explicit_session = explicit_session
        self.__killed = (self.__id == 0)
        if self.__killed:
            self.__end_session(True)

        if "ns" in cursor_info:
            self.__ns = cursor_info["ns"]
        else:
            self.__ns = collection.full_name

        self.batch_size(batch_size)

        if (not isinstance(max_await_time_ms, integer_types)
                and max_await_time_ms is not None):
            raise TypeError("max_await_time_ms must be an integer or None")

    def __del__(self):
        if self.__id and not self.__killed:
            self.__die()

    def __die(self, synchronous=False):
        """Closes this cursor.
        """
        already_killed = self.__killed
        self.__killed = True
        if self.__id and not already_killed:
            address = _CursorAddress(
                self.__address, self.__collection.full_name)
            if synchronous:
                self.__collection.database.client._close_cursor_now(
                    self.__id, address, session=self.__session)
            else:
                # The cursor will be closed later in a different session.
                self.__collection.database.client._close_cursor(
                    self.__id, address)
        self.__end_session(synchronous)

    def __end_session(self, synchronous):
        if self.__session and not self.__explicit_session:
            self.__session._end_session(lock=synchronous)
            self.__session = None

    def close(self):
        """Explicitly close / kill this cursor.
        """
        self.__die(True)

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

        :Parameters:
          - `batch_size`: The size of each batch of results requested.
        """
        if not isinstance(batch_size, integer_types):
            raise TypeError("batch_size must be an integer")
        if batch_size < 0:
            raise ValueError("batch_size must be >= 0")

        self.__batch_size = batch_size == 1 and 2 or batch_size
        return self

    def _has_next(self):
        """Returns `True` if the cursor has documents remaining from the
        previous batch."""
        return len(self.__data) > 0

    @property
    def _post_batch_resume_token(self):
        """Retrieve the postBatchResumeToken from the response to a
        changeStream aggregate or getMore."""
        return self.__postbatchresumetoken

    def __send_message(self, operation):
        """Send a getmore message and handle the response.
        """
        def kill():
            self.__killed = True
            self.__end_session(True)

        client = self.__collection.database.client
        try:
            response = client._run_operation_with_response(
                operation, self._unpack_response, address=self.__address)
        except OperationFailure:
            kill()
            raise
        except NotMasterError:
            # Don't send kill cursors to another server after a "not master"
            # error. It's completely pointless.
            kill()
            raise
        except ConnectionFailure:
            # Don't try to send kill cursors on another socket
            # or to another server. It can cause a _pinValue
            # assertion on some server releases if we get here
            # due to a socket timeout.
            kill()
            raise
        except Exception:
            # Close the cursor
            self.__die()
            raise

        from_command = response.from_command
        reply = response.data
        docs = response.docs

        if from_command:
            cursor = docs[0]['cursor']
            documents = cursor['nextBatch']
            self.__postbatchresumetoken = cursor.get('postBatchResumeToken')
            self.__id = cursor['id']
        else:
            documents = docs
            self.__id = reply.cursor_id

        if self.__id == 0:
            kill()
        self.__data = deque(documents)

    def _unpack_response(self, response, cursor_id, codec_options,
                         user_fields=None, legacy_response=False):
        return response.unpack_response(cursor_id, codec_options, user_fields,
                                        legacy_response)

    def _refresh(self):
        """Refreshes the cursor with more data from the server.

        Returns the length of self.__data after refresh. Will exit early if
        self.__data is already non-empty. Raises OperationFailure when the
        cursor cannot be refreshed due to an error on the query.
        """
        if len(self.__data) or self.__killed:
            return len(self.__data)

        if self.__id:  # Get More
            dbname, collname = self.__ns.split('.', 1)
            read_pref = self.__collection._read_preference_for(self.session)
            self.__send_message(
                self._getmore_class(dbname,
                                    collname,
                                    self.__batch_size,
                                    self.__id,
                                    self.__collection.codec_options,
                                    read_pref,
                                    self.__session,
                                    self.__collection.database.client,
                                    self.__max_await_time_ms,
                                    False))
        else:  # Cursor id is zero nothing else to return
            self.__killed = True
            self.__end_session(True)

        return len(self.__data)

    @property
    def alive(self):
        """Does this cursor have the potential to return more data?

        Even if :attr:`alive` is ``True``, :meth:`next` can raise
        :exc:`StopIteration`. Best to use a for loop::

            for doc in collection.aggregate(pipeline):
                print(doc)

        .. note:: :attr:`alive` can be True while iterating a cursor from
          a failed server. In this case :attr:`alive` will return False after
          :meth:`next` fails to retrieve the next batch of results from the
          server.
        """
        return bool(len(self.__data) or (not self.__killed))

    @property
    def cursor_id(self):
        """Returns the id of the cursor."""
        return self.__id

    @property
    def address(self):
        """The (host, port) of the server used, or None.

        .. versionadded:: 3.0
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
        # Block until a document is returnable.
        while self.alive:
            doc = self._try_next(True)
            if doc is not None:
                return doc

        raise StopIteration

    __next__ = next

    def _try_next(self, get_more_allowed):
        """Advance the cursor blocking for at most one getMore command."""
        if not len(self.__data) and not self.__killed and get_more_allowed:
            self._refresh()
        if len(self.__data):
            coll = self.__collection
            return coll.database._fix_outgoing(self.__data.popleft(), coll)
        else:
            return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class RawBatchCommandCursor(CommandCursor):
    _getmore_class = _RawBatchGetMore

    def __init__(self, collection, cursor_info, address, retrieved=0,
                 batch_size=0, max_await_time_ms=None, session=None,
                 explicit_session=False):
        """Create a new cursor / iterator over raw batches of BSON data.

        Should not be called directly by application developers -
        see :meth:`~pymongo.collection.Collection.aggregate_raw_batches`
        instead.

        .. mongodoc:: cursors
        """
        assert not cursor_info.get('firstBatch')
        super(RawBatchCommandCursor, self).__init__(
            collection, cursor_info, address, retrieved, batch_size,
            max_await_time_ms, session, explicit_session)

    def _unpack_response(self, response, cursor_id, codec_options,
                         user_fields=None, legacy_response=False):
        return response.raw_response(cursor_id)

    def __getitem__(self, index):
        raise InvalidOperation("Cannot call __getitem__ on RawBatchCursor")
