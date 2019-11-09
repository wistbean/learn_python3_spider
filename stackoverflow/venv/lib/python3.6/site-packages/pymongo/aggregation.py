# Copyright 2019-present MongoDB, Inc.
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

"""Perform aggregation operations on a collection or database."""

from bson.son import SON

from pymongo import common
from pymongo.collation import validate_collation_or_none
from pymongo.errors import ConfigurationError
from pymongo.read_preferences import ReadPreference


class _AggregationCommand(object):
    """The internal abstract base class for aggregation cursors.

    Should not be called directly by application developers. Use
    :meth:`pymongo.collection.Collection.aggregate`, or
    :meth:`pymongo.database.Database.aggregate` instead.
    """
    def __init__(self, target, cursor_class, pipeline, options,
                 explicit_session, user_fields=None, result_processor=None):
        if "explain" in options:
            raise ConfigurationError("The explain option is not supported. "
                                     "Use Database.command instead.")

        self._target = target

        common.validate_list('pipeline', pipeline)
        self._pipeline = pipeline
        self._performs_write = False
        if pipeline and ("$out" in pipeline[-1] or "$merge" in pipeline[-1]):
            self._performs_write = True

        common.validate_is_mapping('options', options)
        self._options = options

        # This is the batchSize that will be used for setting the initial
        # batchSize for the cursor, as well as the subsequent getMores.
        self._batch_size = common.validate_non_negative_integer_or_none(
            "batchSize", self._options.pop("batchSize", None))

        # If the cursor option is already specified, avoid overriding it.
        self._options.setdefault("cursor", {})
        # If the pipeline performs a write, we ignore the initial batchSize
        # since the server doesn't return results in this case.
        if self._batch_size is not None and not self._performs_write:
            self._options["cursor"]["batchSize"] = self._batch_size

        self._cursor_class = cursor_class
        self._explicit_session = explicit_session
        self._user_fields = user_fields
        self._result_processor = result_processor

        self._collation = validate_collation_or_none(
            options.pop('collation', None))

        self._max_await_time_ms = options.pop('maxAwaitTimeMS', None)

    @property
    def _aggregation_target(self):
        """The argument to pass to the aggregate command."""
        raise NotImplementedError

    @property
    def _cursor_namespace(self):
        """The namespace in which the aggregate command is run."""
        raise NotImplementedError

    @property
    def _cursor_collection(self, cursor_doc):
        """The Collection used for the aggregate command cursor."""
        raise NotImplementedError

    @property
    def _database(self):
        """The database against which the aggregation command is run."""
        raise NotImplementedError

    @staticmethod
    def _check_compat(sock_info):
        """Check whether the server version in-use supports aggregation."""
        pass

    def _process_result(self, result, session, server, sock_info, slave_ok):
        if self._result_processor:
            self._result_processor(
                result, session, server, sock_info, slave_ok)

    def get_read_preference(self, session):
        if self._performs_write:
            return ReadPreference.PRIMARY
        return self._target._read_preference_for(session)

    def get_cursor(self, session, server, sock_info, slave_ok):
        # Ensure command compatibility.
        self._check_compat(sock_info)

        # Serialize command.
        cmd = SON([("aggregate", self._aggregation_target),
                   ("pipeline", self._pipeline)])
        cmd.update(self._options)

        # Apply this target's read concern if:
        # readConcern has not been specified as a kwarg and either
        # - server version is >= 4.2 or
        # - server version is >= 3.2 and pipeline doesn't use $out
        if (('readConcern' not in cmd) and
                ((sock_info.max_wire_version >= 4 and
                  not self._performs_write) or
                 (sock_info.max_wire_version >= 8))):
            read_concern = self._target.read_concern
        else:
            read_concern = None

        # Apply this target's write concern if:
        # writeConcern has not been specified as a kwarg and pipeline doesn't
        # perform a write operation
        if 'writeConcern' not in cmd and self._performs_write:
            write_concern = self._target._write_concern_for(session)
        else:
            write_concern = None

        # Run command.
        result = sock_info.command(
            self._database.name,
            cmd,
            slave_ok,
            self.get_read_preference(session),
            self._target.codec_options,
            parse_write_concern_error=True,
            read_concern=read_concern,
            write_concern=write_concern,
            collation=self._collation,
            session=session,
            client=self._database.client,
            user_fields=self._user_fields)

        self._process_result(result, session, server, sock_info, slave_ok)

        # Extract cursor from result or mock/fake one if necessary.
        if 'cursor' in result:
            cursor = result['cursor']
        else:
            # Pre-MongoDB 2.6 or unacknowledged write. Fake a cursor.
            cursor = {
                "id": 0,
                "firstBatch": result.get("result", []),
                "ns": self._cursor_namespace,
            }

        # Create and return cursor instance.
        return self._cursor_class(
            self._cursor_collection(cursor), cursor, sock_info.address,
            batch_size=self._batch_size or 0,
            max_await_time_ms=self._max_await_time_ms,
            session=session, explicit_session=self._explicit_session)


class _CollectionAggregationCommand(_AggregationCommand):
    def __init__(self, *args, **kwargs):
        # Pop additional option and initialize parent class.
        use_cursor = kwargs.pop("use_cursor", True)
        super(_CollectionAggregationCommand, self).__init__(*args, **kwargs)

        # Remove the cursor document if the user has set use_cursor to False.
        self._use_cursor = use_cursor
        if not self._use_cursor:
            self._options.pop("cursor", None)

    @property
    def _aggregation_target(self):
        return self._target.name

    @property
    def _cursor_namespace(self):
        return self._target.full_name

    def _cursor_collection(self, cursor):
        """The Collection used for the aggregate command cursor."""
        return self._target

    @property
    def _database(self):
        return self._target.database


class _CollectionRawAggregationCommand(_CollectionAggregationCommand):
    def __init__(self, *args, **kwargs):
        super(_CollectionRawAggregationCommand, self).__init__(*args, **kwargs)

        # For raw-batches, we set the initial batchSize for the cursor to 0.
        if self._use_cursor and not self._performs_write:
            self._options["cursor"]["batchSize"] = 0


class _DatabaseAggregationCommand(_AggregationCommand):
    @property
    def _aggregation_target(self):
        return 1

    @property
    def _cursor_namespace(self):
        return "%s.$cmd.aggregate" % (self._target.name,)

    @property
    def _database(self):
        return self._target

    def _cursor_collection(self, cursor):
        """The Collection used for the aggregate command cursor."""
        # Collection level aggregate may not always return the "ns" field
        # according to our MockupDB tests. Let's handle that case for db level
        # aggregate too by defaulting to the <db>.$cmd.aggregate namespace.
        _, collname = cursor.get("ns", self._cursor_namespace).split(".", 1)
        return self._database[collname]

    @staticmethod
    def _check_compat(sock_info):
        # Older server version don't raise a descriptive error, so we raise
        # one instead.
        if not sock_info.max_wire_version >= 6:
            err_msg = "Database.aggregate() is only supported on MongoDB 3.6+."
            raise ConfigurationError(err_msg)
