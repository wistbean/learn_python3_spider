# Copyright 2017 MongoDB, Inc.
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

"""Watch changes on a collection, a database, or the entire cluster."""

import copy

from bson import _bson_to_dict
from bson.raw_bson import RawBSONDocument

from pymongo import common
from pymongo.aggregation import (_CollectionAggregationCommand,
                                 _DatabaseAggregationCommand)
from pymongo.collation import validate_collation_or_none
from pymongo.command_cursor import CommandCursor
from pymongo.errors import (ConnectionFailure,
                            InvalidOperation,
                            OperationFailure,
                            PyMongoError)


# The change streams spec considers the following server errors from the
# getMore command non-resumable. All other getMore errors are resumable.
_NON_RESUMABLE_GETMORE_ERRORS = frozenset([
    11601,  # Interrupted
    136,    # CappedPositionLost
    237,    # CursorKilled
    None,   # No error code was returned.
])


class ChangeStream(object):
    """The internal abstract base class for change stream cursors.

    Should not be called directly by application developers. Use 
    :meth:`pymongo.collection.Collection.watch`,
    :meth:`pymongo.database.Database.watch`, or
    :meth:`pymongo.mongo_client.MongoClient.watch` instead.

    .. versionadded:: 3.6
    .. mongodoc:: changeStreams
    """
    def __init__(self, target, pipeline, full_document, resume_after,
                 max_await_time_ms, batch_size, collation,
                 start_at_operation_time, session, start_after):
        if pipeline is None:
            pipeline = []
        elif not isinstance(pipeline, list):
            raise TypeError("pipeline must be a list")

        common.validate_string_or_none('full_document', full_document)
        validate_collation_or_none(collation)
        common.validate_non_negative_integer_or_none("batchSize", batch_size)

        self._decode_custom = False
        self._orig_codec_options = target.codec_options
        if target.codec_options.type_registry._decoder_map:
            self._decode_custom = True
            # Keep the type registry so that we support encoding custom types
            # in the pipeline.
            self._target = target.with_options(
                codec_options=target.codec_options.with_options(
                    document_class=RawBSONDocument))
        else:
            self._target = target

        self._pipeline = copy.deepcopy(pipeline)
        self._full_document = full_document
        self._uses_start_after = start_after is not None
        self._uses_resume_after = resume_after is not None
        self._resume_token = copy.deepcopy(start_after or resume_after)
        self._max_await_time_ms = max_await_time_ms
        self._batch_size = batch_size
        self._collation = collation
        self._start_at_operation_time = start_at_operation_time
        self._session = session

        # Initialize cursor.
        self._cursor = self._create_cursor()

    @property
    def _aggregation_command_class(self):
        """The aggregation command class to be used."""
        raise NotImplementedError

    @property
    def _client(self):
        """The client against which the aggregation commands for
        this ChangeStream will be run. """
        raise NotImplementedError

    def _change_stream_options(self):
        """Return the options dict for the $changeStream pipeline stage."""
        options = {}
        if self._full_document is not None:
            options['fullDocument'] = self._full_document

        resume_token = self.resume_token
        if resume_token is not None:
            if self._uses_start_after:
                options['startAfter'] = resume_token
            if self._uses_resume_after:
                options['resumeAfter'] = resume_token

        if self._start_at_operation_time is not None:
            options['startAtOperationTime'] = self._start_at_operation_time
        return options

    def _command_options(self):
        """Return the options dict for the aggregation command."""
        options = {}
        if self._max_await_time_ms is not None:
            options["maxAwaitTimeMS"] = self._max_await_time_ms
        if self._batch_size is not None:
            options["batchSize"] = self._batch_size
        return options

    def _aggregation_pipeline(self):
        """Return the full aggregation pipeline for this ChangeStream."""
        options = self._change_stream_options()
        full_pipeline = [{'$changeStream': options}]
        full_pipeline.extend(self._pipeline)
        return full_pipeline

    def _process_result(self, result, session, server, sock_info, slave_ok):
        """Callback that caches the startAtOperationTime from a changeStream
        aggregate command response containing an empty batch of change
        documents.

        This is implemented as a callback because we need access to the wire
        version in order to determine whether to cache this value.
        """
        if not result['cursor']['firstBatch']:
            if (self._start_at_operation_time is None and
                    self.resume_token is None and
                    sock_info.max_wire_version >= 7):
                self._start_at_operation_time = result["operationTime"]

    def _run_aggregation_cmd(self, session, explicit_session):
        """Run the full aggregation pipeline for this ChangeStream and return
        the corresponding CommandCursor.
        """
        cmd = self._aggregation_command_class(
            self._target, CommandCursor, self._aggregation_pipeline(),
            self._command_options(), explicit_session,
            result_processor=self._process_result)

        return self._client._retryable_read(
            cmd.get_cursor, self._target._read_preference_for(session),
            session)

    def _create_cursor(self):
        with self._client._tmp_session(self._session, close=False) as s:
            return self._run_aggregation_cmd(
                session=s,
                explicit_session=self._session is not None)

    def _resume(self):
        """Reestablish this change stream after a resumable error."""
        try:
            self._cursor.close()
        except PyMongoError:
            pass
        self._cursor = self._create_cursor()

    def close(self):
        """Close this ChangeStream."""
        self._cursor.close()

    def __iter__(self):
        return self

    @property
    def resume_token(self):
        """The cached resume token that will be used to resume after the most
        recently returned change.

        .. versionadded:: 3.9
        """
        return copy.deepcopy(self._resume_token)

    def next(self):
        """Advance the cursor.

        This method blocks until the next change document is returned or an
        unrecoverable error is raised. This method is used when iterating over
        all changes in the cursor. For example::

            try:
                resume_token = None
                pipeline = [{'$match': {'operationType': 'insert'}}]
                with db.collection.watch(pipeline) as stream:
                    for insert_change in stream:
                        print(insert_change)
                        resume_token = stream.resume_token
            except pymongo.errors.PyMongoError:
                # The ChangeStream encountered an unrecoverable error or the
                # resume attempt failed to recreate the cursor.
                if resume_token is None:
                    # There is no usable resume token because there was a
                    # failure during ChangeStream initialization.
                    logging.error('...')
                else:
                    # Use the interrupted ChangeStream's resume token to create
                    # a new ChangeStream. The new stream will continue from the
                    # last seen insert change without missing any events.
                    with db.collection.watch(
                            pipeline, resume_after=resume_token) as stream:
                        for insert_change in stream:
                            print(insert_change)

        Raises :exc:`StopIteration` if this ChangeStream is closed.
        """
        while self.alive:
            doc = self.try_next()
            if doc is not None:
                return doc

        raise StopIteration

    __next__ = next

    @property
    def alive(self):
        """Does this cursor have the potential to return more data?

        .. note:: Even if :attr:`alive` is ``True``, :meth:`next` can raise
            :exc:`StopIteration` and :meth:`try_next` can return ``None``.

        .. versionadded:: 3.8
        """
        return self._cursor.alive

    def try_next(self):
        """Advance the cursor without blocking indefinitely.

        This method returns the next change document without waiting
        indefinitely for the next change. For example::

            with db.collection.watch() as stream:
                while stream.alive:
                    change = stream.try_next()
                    # Note that the ChangeStream's resume token may be updated
                    # even when no changes are returned.
                    print("Current resume token: %r" % (stream.resume_token,))
                    if change is not None:
                        print("Change document: %r" % (change,))
                        continue
                    # We end up here when there are no recent changes.
                    # Sleep for a while before trying again to avoid flooding
                    # the server with getMore requests when no changes are
                    # available.
                    time.sleep(10)

        If no change document is cached locally then this method runs a single
        getMore command. If the getMore yields any documents, the next
        document is returned, otherwise, if the getMore returns no documents
        (because there have been no changes) then ``None`` is returned.

        :Returns:
          The next change document or ``None`` when no document is available
          after running a single getMore or when the cursor is closed.

        .. versionadded:: 3.8
        """
        # Attempt to get the next change with at most one getMore and at most
        # one resume attempt.
        try:
            change = self._cursor._try_next(True)
        except ConnectionFailure:
            self._resume()
            change = self._cursor._try_next(False)
        except OperationFailure as exc:
            if (exc.code in _NON_RESUMABLE_GETMORE_ERRORS or
                    exc.has_error_label("NonResumableChangeStreamError")):
                raise
            self._resume()
            change = self._cursor._try_next(False)

        # If no changes are available.
        if change is None:
            # We have either iterated over all documents in the cursor,
            # OR the most-recently returned batch is empty. In either case,
            # update the cached resume token with the postBatchResumeToken if
            # one was returned. We also clear the startAtOperationTime.
            if self._cursor._post_batch_resume_token is not None:
                self._resume_token = self._cursor._post_batch_resume_token
                self._start_at_operation_time = None
            return change

        # Else, changes are available.
        try:
            resume_token = change['_id']
        except KeyError:
            self.close()
            raise InvalidOperation(
                "Cannot provide resume functionality when the resume "
                "token is missing.")

        # If this is the last change document from the current batch, cache the
        # postBatchResumeToken.
        if (not self._cursor._has_next() and
                self._cursor._post_batch_resume_token):
            resume_token = self._cursor._post_batch_resume_token

        # Hereafter, don't use startAfter; instead use resumeAfter.
        self._uses_start_after = False
        self._uses_resume_after = True

        # Cache the resume token and clear startAtOperationTime.
        self._resume_token = resume_token
        self._start_at_operation_time = None

        if self._decode_custom:
            return _bson_to_dict(change.raw, self._orig_codec_options)
        return change

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class CollectionChangeStream(ChangeStream):
    """A change stream that watches changes on a single collection.

    Should not be called directly by application developers. Use
    helper method :meth:`pymongo.collection.Collection.watch` instead.

    .. versionadded:: 3.7
    """
    @property
    def _aggregation_command_class(self):
        return _CollectionAggregationCommand

    @property
    def _client(self):
        return self._target.database.client


class DatabaseChangeStream(ChangeStream):
    """A change stream that watches changes on all collections in a database.

    Should not be called directly by application developers. Use
    helper method :meth:`pymongo.database.Database.watch` instead.

    .. versionadded:: 3.7
    """
    @property
    def _aggregation_command_class(self):
        return _DatabaseAggregationCommand

    @property
    def _client(self):
        return self._target.client


class ClusterChangeStream(DatabaseChangeStream):
    """A change stream that watches changes on all collections in the cluster.

    Should not be called directly by application developers. Use
    helper method :meth:`pymongo.mongo_client.MongoClient.watch` instead.

    .. versionadded:: 3.7
    """
    def _change_stream_options(self):
        options = super(ClusterChangeStream, self)._change_stream_options()
        options["allChangesForCluster"] = True
        return options
