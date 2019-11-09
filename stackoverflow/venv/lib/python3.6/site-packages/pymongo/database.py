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

"""Database level operations."""

import warnings

from bson.code import Code
from bson.codec_options import DEFAULT_CODEC_OPTIONS
from bson.dbref import DBRef
from bson.py3compat import iteritems, string_type, _unicode
from bson.son import SON
from pymongo import auth, common
from pymongo.aggregation import _DatabaseAggregationCommand
from pymongo.change_stream import DatabaseChangeStream
from pymongo.collection import Collection
from pymongo.command_cursor import CommandCursor
from pymongo.errors import (CollectionInvalid,
                            ConfigurationError,
                            InvalidName,
                            OperationFailure)
from pymongo.message import _first_batch
from pymongo.read_preferences import ReadPreference
from pymongo.son_manipulator import SONManipulator
from pymongo.write_concern import DEFAULT_WRITE_CONCERN


_INDEX_REGEX = {"name": {"$regex": r"^(?!.*\$)"}}
_SYSTEM_FILTER = {"filter": {"name": {"$regex": r"^(?!system\.)"}}}


def _check_name(name):
    """Check if a database name is valid.
    """
    if not name:
        raise InvalidName("database name cannot be the empty string")

    for invalid_char in [' ', '.', '$', '/', '\\', '\x00', '"']:
        if invalid_char in name:
            raise InvalidName("database names cannot contain the "
                              "character %r" % invalid_char)


class Database(common.BaseObject):
    """A Mongo database.
    """

    def __init__(self, client, name, codec_options=None, read_preference=None,
                 write_concern=None, read_concern=None):
        """Get a database by client and name.

        Raises :class:`TypeError` if `name` is not an instance of
        :class:`basestring` (:class:`str` in python 3). Raises
        :class:`~pymongo.errors.InvalidName` if `name` is not a valid
        database name.

        :Parameters:
          - `client`: A :class:`~pymongo.mongo_client.MongoClient` instance.
          - `name`: The database name.
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) client.codec_options is used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) client.read_preference is used.
          - `write_concern` (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) client.write_concern is used.
          - `read_concern` (optional): An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) client.read_concern is used.

        .. mongodoc:: databases

        .. versionchanged:: 3.2
           Added the read_concern option.

        .. versionchanged:: 3.0
           Added the codec_options, read_preference, and write_concern options.
           :class:`~pymongo.database.Database` no longer returns an instance
           of :class:`~pymongo.collection.Collection` for attribute names
           with leading underscores. You must use dict-style lookups instead::

               db['__my_collection__']

           Not:

               db.__my_collection__
        """
        super(Database, self).__init__(
            codec_options or client.codec_options,
            read_preference or client.read_preference,
            write_concern or client.write_concern,
            read_concern or client.read_concern)

        if not isinstance(name, string_type):
            raise TypeError("name must be an instance "
                            "of %s" % (string_type.__name__,))

        if name != '$external':
            _check_name(name)

        self.__name = _unicode(name)
        self.__client = client

        self.__incoming_manipulators = []
        self.__incoming_copying_manipulators = []
        self.__outgoing_manipulators = []
        self.__outgoing_copying_manipulators = []

    def add_son_manipulator(self, manipulator):
        """Add a new son manipulator to this database.

        **DEPRECATED** - `add_son_manipulator` is deprecated.

        .. versionchanged:: 3.0
          Deprecated add_son_manipulator.
        """
        warnings.warn("add_son_manipulator is deprecated",
                      DeprecationWarning, stacklevel=2)
        base = SONManipulator()
        def method_overwritten(instance, method):
            """Test if this method has been overridden."""
            return (getattr(
                instance, method).__func__ != getattr(base, method).__func__)

        if manipulator.will_copy():
            if method_overwritten(manipulator, "transform_incoming"):
                self.__incoming_copying_manipulators.insert(0, manipulator)
            if method_overwritten(manipulator, "transform_outgoing"):
                self.__outgoing_copying_manipulators.insert(0, manipulator)
        else:
            if method_overwritten(manipulator, "transform_incoming"):
                self.__incoming_manipulators.insert(0, manipulator)
            if method_overwritten(manipulator, "transform_outgoing"):
                self.__outgoing_manipulators.insert(0, manipulator)

    @property
    def system_js(self):
        """**DEPRECATED**: :class:`SystemJS` helper for this :class:`Database`.

        See the documentation for :class:`SystemJS` for more details.
        """
        return SystemJS(self)

    @property
    def client(self):
        """The client instance for this :class:`Database`."""
        return self.__client

    @property
    def name(self):
        """The name of this :class:`Database`."""
        return self.__name

    @property
    def incoming_manipulators(self):
        """**DEPRECATED**: All incoming SON manipulators.

        .. versionchanged:: 3.5
          Deprecated.

        .. versionadded:: 2.0
        """
        warnings.warn("Database.incoming_manipulators() is deprecated",
                      DeprecationWarning, stacklevel=2)

        return [manipulator.__class__.__name__
                for manipulator in self.__incoming_manipulators]

    @property
    def incoming_copying_manipulators(self):
        """**DEPRECATED**: All incoming SON copying manipulators.

        .. versionchanged:: 3.5
          Deprecated.

        .. versionadded:: 2.0
        """
        warnings.warn("Database.incoming_copying_manipulators() is deprecated",
                      DeprecationWarning, stacklevel=2)

        return [manipulator.__class__.__name__
                for manipulator in self.__incoming_copying_manipulators]

    @property
    def outgoing_manipulators(self):
        """**DEPRECATED**: All outgoing SON manipulators.

        .. versionchanged:: 3.5
          Deprecated.

        .. versionadded:: 2.0
        """
        warnings.warn("Database.outgoing_manipulators() is deprecated",
                      DeprecationWarning, stacklevel=2)

        return [manipulator.__class__.__name__
                for manipulator in self.__outgoing_manipulators]

    @property
    def outgoing_copying_manipulators(self):
        """**DEPRECATED**: All outgoing SON copying manipulators.

        .. versionchanged:: 3.5
          Deprecated.

        .. versionadded:: 2.0
        """
        warnings.warn("Database.outgoing_copying_manipulators() is deprecated",
                      DeprecationWarning, stacklevel=2)

        return [manipulator.__class__.__name__
                for manipulator in self.__outgoing_copying_manipulators]

    def with_options(self, codec_options=None, read_preference=None,
                     write_concern=None, read_concern=None):
        """Get a clone of this database changing the specified settings.

          >>> db1.read_preference
          Primary()
          >>> from pymongo import ReadPreference
          >>> db2 = db1.with_options(read_preference=ReadPreference.SECONDARY)
          >>> db1.read_preference
          Primary()
          >>> db2.read_preference
          Secondary(tag_sets=None)

        :Parameters:
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`Collection`
            is used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`Collection` is used. See :mod:`~pymongo.read_preferences`
            for options.
          - `write_concern` (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`Collection`
            is used.
          - `read_concern` (optional): An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`Collection`
            is used.

        .. versionadded:: 3.8
        """
        return Database(self.client,
                        self.__name,
                        codec_options or self.codec_options,
                        read_preference or self.read_preference,
                        write_concern or self.write_concern,
                        read_concern or self.read_concern)

    def __eq__(self, other):
        if isinstance(other, Database):
            return (self.__client == other.client and
                    self.__name == other.name)
        return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "Database(%r, %r)" % (self.__client, self.__name)

    def __getattr__(self, name):
        """Get a collection of this database by name.

        Raises InvalidName if an invalid collection name is used.

        :Parameters:
          - `name`: the name of the collection to get
        """
        if name.startswith('_'):
            raise AttributeError(
                "Database has no attribute %r. To access the %s"
                " collection, use database[%r]." % (name, name, name))
        return self.__getitem__(name)

    def __getitem__(self, name):
        """Get a collection of this database by name.

        Raises InvalidName if an invalid collection name is used.

        :Parameters:
          - `name`: the name of the collection to get
        """
        return Collection(self, name)

    def get_collection(self, name, codec_options=None, read_preference=None,
                       write_concern=None, read_concern=None):
        """Get a :class:`~pymongo.collection.Collection` with the given name
        and options.

        Useful for creating a :class:`~pymongo.collection.Collection` with
        different codec options, read preference, and/or write concern from
        this :class:`Database`.

          >>> db.read_preference
          Primary()
          >>> coll1 = db.test
          >>> coll1.read_preference
          Primary()
          >>> from pymongo import ReadPreference
          >>> coll2 = db.get_collection(
          ...     'test', read_preference=ReadPreference.SECONDARY)
          >>> coll2.read_preference
          Secondary(tag_sets=None)

        :Parameters:
          - `name`: The name of the collection - a string.
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`Database` is
            used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`Database` is used. See :mod:`~pymongo.read_preferences`
            for options.
          - `write_concern` (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`Database` is
            used.
          - `read_concern` (optional): An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`Database` is
            used.
        """
        return Collection(
            self, name, False, codec_options, read_preference,
            write_concern, read_concern)

    def create_collection(self, name, codec_options=None,
                          read_preference=None, write_concern=None,
                          read_concern=None, session=None, **kwargs):
        """Create a new :class:`~pymongo.collection.Collection` in this
        database.

        Normally collection creation is automatic. This method should
        only be used to specify options on
        creation. :class:`~pymongo.errors.CollectionInvalid` will be
        raised if the collection already exists.

        Options should be passed as keyword arguments to this method. Supported
        options vary with MongoDB release. Some examples include:

          - "size": desired initial size for the collection (in
            bytes). For capped collections this size is the max
            size of the collection.
          - "capped": if True, this is a capped collection
          - "max": maximum number of objects if capped (optional)

        See the MongoDB documentation for a full list of supported options by
        server version.

        :Parameters:
          - `name`: the name of the collection to create
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`Database` is
            used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`Database` is used.
          - `write_concern` (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`Database` is
            used.
          - `read_concern` (optional): An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`Database` is
            used.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): additional keyword arguments will
            be passed as options for the create collection command

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Added the collation option.

        .. versionchanged:: 3.0
           Added the codec_options, read_preference, and write_concern options.

        .. versionchanged:: 2.2
           Removed deprecated argument: options
        """
        with self.__client._tmp_session(session) as s:
            if name in self.list_collection_names(
                    filter={"name": name}, session=s):
                raise CollectionInvalid("collection %s already exists" % name)

            return Collection(self, name, True, codec_options,
                              read_preference, write_concern,
                              read_concern, session=s, **kwargs)

    def _apply_incoming_manipulators(self, son, collection):
        """Apply incoming manipulators to `son`."""
        for manipulator in self.__incoming_manipulators:
            son = manipulator.transform_incoming(son, collection)
        return son

    def _apply_incoming_copying_manipulators(self, son, collection):
        """Apply incoming copying manipulators to `son`."""
        for manipulator in self.__incoming_copying_manipulators:
            son = manipulator.transform_incoming(son, collection)
        return son

    def _fix_incoming(self, son, collection):
        """Apply manipulators to an incoming SON object before it gets stored.

        :Parameters:
          - `son`: the son object going into the database
          - `collection`: the collection the son object is being saved in
        """
        son = self._apply_incoming_manipulators(son, collection)
        son = self._apply_incoming_copying_manipulators(son, collection)
        return son

    def _fix_outgoing(self, son, collection):
        """Apply manipulators to a SON object as it comes out of the database.

        :Parameters:
          - `son`: the son object coming out of the database
          - `collection`: the collection the son object was saved in
        """
        for manipulator in reversed(self.__outgoing_manipulators):
            son = manipulator.transform_outgoing(son, collection)
        for manipulator in reversed(self.__outgoing_copying_manipulators):
            son = manipulator.transform_outgoing(son, collection)
        return son

    def aggregate(self, pipeline, session=None, **kwargs):
        """Perform a database-level aggregation.

        See the `aggregation pipeline`_ documentation for a list of stages
        that are supported.

        Introduced in MongoDB 3.6.

        .. code-block:: python

           # Lists all operations currently running on the server.
           with client.admin.aggregate([{"$currentOp": {}}]) as cursor:
               for operation in cursor:
                   print(operation)

        All optional `aggregate command`_ parameters should be passed as
        keyword arguments to this method. Valid options include, but are not
        limited to:

          - `allowDiskUse` (bool): Enables writing to temporary files. When set
            to True, aggregation stages can write data to the _tmp subdirectory
            of the --dbpath directory. The default is False.
          - `maxTimeMS` (int): The maximum amount of time to allow the operation
            to run in milliseconds.
          - `batchSize` (int): The maximum number of documents to return per
            batch. Ignored if the connected mongod or mongos does not support
            returning aggregate results using a cursor.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.

        The :meth:`aggregate` method obeys the :attr:`read_preference` of this
        :class:`Database`, except when ``$out`` or ``$merge`` are used, in
        which case  :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY`
        is used.

        .. note:: This method does not support the 'explain' option. Please
           use :meth:`~pymongo.database.Database.command` instead.

        .. note:: The :attr:`~pymongo.database.Database.write_concern` of
           this collection is automatically applied to this operation.

        :Parameters:
          - `pipeline`: a list of aggregation pipeline stages
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): See list of options above.

        :Returns:
          A :class:`~pymongo.command_cursor.CommandCursor` over the result
          set.

        .. versionadded:: 3.9

        .. _aggregation pipeline:
            https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline

        .. _aggregate command:
            https://docs.mongodb.com/manual/reference/command/aggregate
        """
        with self.client._tmp_session(session, close=False) as s:
            cmd = _DatabaseAggregationCommand(
                self, CommandCursor, pipeline, kwargs, session is not None,
                user_fields={'cursor': {'firstBatch': 1}})
            return self.client._retryable_read(
                cmd.get_cursor, cmd.get_read_preference(s), s,
                retryable=not cmd._performs_write)

    def watch(self, pipeline=None, full_document=None, resume_after=None,
              max_await_time_ms=None, batch_size=None, collation=None,
              start_at_operation_time=None, session=None, start_after=None):
        """Watch changes on this database.

        Performs an aggregation with an implicit initial ``$changeStream``
        stage and returns a
        :class:`~pymongo.change_stream.DatabaseChangeStream` cursor which
        iterates over changes on all collections in this database.

        Introduced in MongoDB 4.0.

        .. code-block:: python

           with db.watch() as stream:
               for change in stream:
                   print(change)

        The :class:`~pymongo.change_stream.DatabaseChangeStream` iterable
        blocks until the next change document is returned or an error is
        raised. If the
        :meth:`~pymongo.change_stream.DatabaseChangeStream.next` method
        encounters a network error when retrieving a batch from the server,
        it will automatically attempt to recreate the cursor such that no
        change events are missed. Any error encountered during the resume
        attempt indicates there may be an outage and will be raised.

        .. code-block:: python

            try:
                with db.watch(
                        [{'$match': {'operationType': 'insert'}}]) as stream:
                    for insert_change in stream:
                        print(insert_change)
            except pymongo.errors.PyMongoError:
                # The ChangeStream encountered an unrecoverable error or the
                # resume attempt failed to recreate the cursor.
                logging.error('...')

        For a precise description of the resume process see the
        `change streams specification`_.

        :Parameters:
          - `pipeline` (optional): A list of aggregation pipeline stages to
            append to an initial ``$changeStream`` stage. Not all
            pipeline stages are valid after a ``$changeStream`` stage, see the
            MongoDB documentation on change streams for the supported stages.
          - `full_document` (optional): The fullDocument to pass as an option
            to the ``$changeStream`` stage. Allowed values: 'updateLookup'.
            When set to 'updateLookup', the change notification for partial
            updates will include both a delta describing the changes to the
            document, as well as a copy of the entire document that was
            changed from some time after the change occurred.
          - `resume_after` (optional): A resume token. If provided, the
            change stream will start returning changes that occur directly
            after the operation specified in the resume token. A resume token
            is the _id value of a change document.
          - `max_await_time_ms` (optional): The maximum time in milliseconds
            for the server to wait for changes before responding to a getMore
            operation.
          - `batch_size` (optional): The maximum number of documents to return
            per batch.
          - `collation` (optional): The :class:`~pymongo.collation.Collation`
            to use for the aggregation.
          - `start_at_operation_time` (optional): If provided, the resulting
            change stream will only return changes that occurred at or after
            the specified :class:`~bson.timestamp.Timestamp`. Requires
            MongoDB >= 4.0.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `start_after` (optional): The same as `resume_after` except that
            `start_after` can resume notifications after an invalidate event.
            This option and `resume_after` are mutually exclusive.

        :Returns:
          A :class:`~pymongo.change_stream.DatabaseChangeStream` cursor.

        .. versionchanged:: 3.9
           Added the ``start_after`` parameter.

        .. versionadded:: 3.7

        .. mongodoc:: changeStreams

        .. _change streams specification:
            https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst
        """
        return DatabaseChangeStream(
            self, pipeline, full_document, resume_after, max_await_time_ms,
            batch_size, collation, start_at_operation_time, session,
            start_after)

    def _command(self, sock_info, command, slave_ok=False, value=1, check=True,
                 allowable_errors=None, read_preference=ReadPreference.PRIMARY,
                 codec_options=DEFAULT_CODEC_OPTIONS,
                 write_concern=None,
                 parse_write_concern_error=False, session=None, **kwargs):
        """Internal command helper."""
        if isinstance(command, string_type):
            command = SON([(command, value)])

        command.update(kwargs)
        with self.__client._tmp_session(session) as s:
            return sock_info.command(
                self.__name,
                command,
                slave_ok,
                read_preference,
                codec_options,
                check,
                allowable_errors,
                write_concern=write_concern,
                parse_write_concern_error=parse_write_concern_error,
                session=s,
                client=self.__client)

    def command(self, command, value=1, check=True,
                allowable_errors=None, read_preference=None,
                codec_options=DEFAULT_CODEC_OPTIONS, session=None, **kwargs):
        """Issue a MongoDB command.

        Send command `command` to the database and return the
        response. If `command` is an instance of :class:`basestring`
        (:class:`str` in python 3) then the command {`command`: `value`}
        will be sent. Otherwise, `command` must be an instance of
        :class:`dict` and will be sent as is.

        Any additional keyword arguments will be added to the final
        command document before it is sent.

        For example, a command like ``{buildinfo: 1}`` can be sent
        using:

        >>> db.command("buildinfo")

        For a command where the value matters, like ``{collstats:
        collection_name}`` we can do:

        >>> db.command("collstats", collection_name)

        For commands that take additional arguments we can use
        kwargs. So ``{filemd5: object_id, root: file_root}`` becomes:

        >>> db.command("filemd5", object_id, root=file_root)

        :Parameters:
          - `command`: document representing the command to be issued,
            or the name of the command (for simple commands only).

            .. note:: the order of keys in the `command` document is
               significant (the "verb" must come first), so commands
               which require multiple keys (e.g. `findandmodify`)
               should use an instance of :class:`~bson.son.SON` or
               a string and kwargs instead of a Python `dict`.

          - `value` (optional): value to use for the command verb when
            `command` is passed as a string
          - `check` (optional): check the response for errors, raising
            :class:`~pymongo.errors.OperationFailure` if there are any
          - `allowable_errors`: if `check` is ``True``, error messages
            in this list will be ignored by error-checking
          - `read_preference` (optional): The read preference for this
            operation. See :mod:`~pymongo.read_preferences` for options.
            If the provided `session` is in a transaction, defaults to the
            read preference configured for the transaction.
            Otherwise, defaults to
            :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY`.
          - `codec_options`: A :class:`~bson.codec_options.CodecOptions`
            instance.
          - `session` (optional): A
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): additional keyword arguments will
            be added to the command document before it is sent

        .. note:: :meth:`command` does **not** obey this Database's
           :attr:`read_preference` or :attr:`codec_options`. You must use the
           `read_preference` and `codec_options` parameters instead.

        .. note:: :meth:`command` does **not** apply any custom TypeDecoders
           when decoding the command response.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.0
           Removed the `as_class`, `fields`, `uuid_subtype`, `tag_sets`,
           and `secondary_acceptable_latency_ms` option.
           Removed `compile_re` option: PyMongo now always represents BSON
           regular expressions as :class:`~bson.regex.Regex` objects. Use
           :meth:`~bson.regex.Regex.try_compile` to attempt to convert from a
           BSON regular expression to a Python regular expression object.
           Added the `codec_options` parameter.

        .. versionchanged:: 2.7
           Added `compile_re` option. If set to False, PyMongo represented BSON
           regular expressions as :class:`~bson.regex.Regex` objects instead of
           attempting to compile BSON regular expressions as Python native
           regular expressions, thus preventing errors for some incompatible
           patterns, see `PYTHON-500`_.

        .. versionchanged:: 2.3
           Added `tag_sets` and `secondary_acceptable_latency_ms` options.
        .. versionchanged:: 2.2
           Added support for `as_class` - the class you want to use for
           the resulting documents

        .. _PYTHON-500: https://jira.mongodb.org/browse/PYTHON-500

        .. mongodoc:: commands
        """
        if read_preference is None:
            read_preference = ((session and session._txn_read_preference())
                               or ReadPreference.PRIMARY)
        with self.__client._socket_for_reads(
                read_preference, session) as (sock_info, slave_ok):
            return self._command(sock_info, command, slave_ok, value,
                                 check, allowable_errors, read_preference,
                                 codec_options, session=session, **kwargs)

    def _retryable_read_command(self, command, value=1, check=True,
                allowable_errors=None, read_preference=None,
                codec_options=DEFAULT_CODEC_OPTIONS, session=None, **kwargs):
        """Same as command but used for retryable read commands."""
        if read_preference is None:
            read_preference = ((session and session._txn_read_preference())
                               or ReadPreference.PRIMARY)

        def _cmd(session, server, sock_info, slave_ok):
            return self._command(sock_info, command, slave_ok, value,
                                 check, allowable_errors, read_preference,
                                 codec_options, session=session, **kwargs)

        return self.__client._retryable_read(
            _cmd, read_preference, session)

    def _list_collections(self, sock_info, slave_okay, session,
                          read_preference, **kwargs):
        """Internal listCollections helper."""

        coll = self.get_collection(
            "$cmd", read_preference=read_preference)
        if sock_info.max_wire_version > 2:
            cmd = SON([("listCollections", 1),
                       ("cursor", {})])
            cmd.update(kwargs)
            with self.__client._tmp_session(
                    session, close=False) as tmp_session:
                cursor = self._command(
                    sock_info, cmd, slave_okay,
                    read_preference=read_preference,
                    session=tmp_session)["cursor"]
                return CommandCursor(
                    coll,
                    cursor,
                    sock_info.address,
                    session=tmp_session,
                    explicit_session=session is not None)
        else:
            match = _INDEX_REGEX
            if "filter" in kwargs:
                match = {"$and": [_INDEX_REGEX, kwargs["filter"]]}
            dblen = len(self.name.encode("utf8") + b".")
            pipeline = [
                {"$project": {"name": {"$substr": ["$name", dblen, -1]},
                              "options": 1}},
                {"$match": match}
            ]
            cmd = SON([("aggregate", "system.namespaces"),
                       ("pipeline", pipeline),
                       ("cursor", kwargs.get("cursor", {}))])
            cursor = self._command(sock_info, cmd, slave_okay)["cursor"]
            return CommandCursor(coll, cursor, sock_info.address)

    def list_collections(self, session=None, filter=None, **kwargs):
        """Get a cursor over the collectons of this database.

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `filter` (optional):  A query document to filter the list of
            collections returned from the listCollections command.
          - `**kwargs` (optional): Optional parameters of the
            `listCollections command
            <https://docs.mongodb.com/manual/reference/command/listCollections/>`_
            can be passed as keyword arguments to this method. The supported
            options differ by server version.

        :Returns:
          An instance of :class:`~pymongo.command_cursor.CommandCursor`.

        .. versionadded:: 3.6
        """
        if filter is not None:
            kwargs['filter'] = filter
        read_pref = ((session and session._txn_read_preference())
                     or ReadPreference.PRIMARY)

        def _cmd(session, server, sock_info, slave_okay):
            return self._list_collections(
                sock_info, slave_okay, session, read_preference=read_pref,
                **kwargs)

        return self.__client._retryable_read(
            _cmd, read_pref, session)

    def list_collection_names(self, session=None, filter=None, **kwargs):
        """Get a list of all the collection names in this database.

        For example, to list all non-system collections::

            filter = {"name": {"$regex": r"^(?!system\.)"}}
            db.list_collection_names(filter=filter)

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `filter` (optional):  A query document to filter the list of
            collections returned from the listCollections command.
          - `**kwargs` (optional): Optional parameters of the
            `listCollections command
            <https://docs.mongodb.com/manual/reference/command/listCollections/>`_
            can be passed as keyword arguments to this method. The supported
            options differ by server version.

        .. versionchanged:: 3.8
           Added the ``filter`` and ``**kwargs`` parameters.

        .. versionadded:: 3.6
        """
        if filter is None:
            kwargs["nameOnly"] = True
        else:
            # The enumerate collections spec states that "drivers MUST NOT set
            # nameOnly if a filter specifies any keys other than name."
            common.validate_is_mapping("filter", filter)
            kwargs["filter"] = filter
            if not filter or (len(filter) == 1 and "name" in filter):
                kwargs["nameOnly"] = True

        return [result["name"]
                for result in self.list_collections(session=session, **kwargs)]

    def collection_names(self, include_system_collections=True,
                         session=None):
        """**DEPRECATED**: Get a list of all the collection names in this
        database.

        :Parameters:
          - `include_system_collections` (optional): if ``False`` list
            will not include system collections (e.g ``system.indexes``)
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        .. versionchanged:: 3.7
           Deprecated. Use :meth:`list_collection_names` instead.

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        warnings.warn("collection_names is deprecated. Use "
                      "list_collection_names instead.",
                      DeprecationWarning, stacklevel=2)
        kws = {} if include_system_collections else _SYSTEM_FILTER
        return [result["name"]
                for result in self.list_collections(session=session,
                                                    nameOnly=True, **kws)]

    def drop_collection(self, name_or_collection, session=None):
        """Drop a collection.

        :Parameters:
          - `name_or_collection`: the name of a collection to drop or the
            collection object itself
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        .. note:: The :attr:`~pymongo.database.Database.write_concern` of
           this database is automatically applied to this operation when using
           MongoDB >= 3.4.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Apply this database's write concern automatically to this operation
           when connected to MongoDB >= 3.4.

        """
        name = name_or_collection
        if isinstance(name, Collection):
            name = name.name

        if not isinstance(name, string_type):
            raise TypeError("name_or_collection must be an "
                            "instance of %s" % (string_type.__name__,))

        self.__client._purge_index(self.__name, name)

        with self.__client._socket_for_writes(session) as sock_info:
            return self._command(
                sock_info, 'drop', value=_unicode(name),
                allowable_errors=['ns not found'],
                write_concern=self._write_concern_for(session),
                parse_write_concern_error=True,
                session=session)

    def validate_collection(self, name_or_collection,
                            scandata=False, full=False, session=None):
        """Validate a collection.

        Returns a dict of validation info. Raises CollectionInvalid if
        validation fails.

        :Parameters:
          - `name_or_collection`: A Collection object or the name of a
            collection to validate.
          - `scandata`: Do extra checks beyond checking the overall
            structure of the collection.
          - `full`: Have the server do a more thorough scan of the
            collection. Use with `scandata` for a thorough scan
            of the structure of the collection and the individual
            documents.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        name = name_or_collection
        if isinstance(name, Collection):
            name = name.name

        if not isinstance(name, string_type):
            raise TypeError("name_or_collection must be an instance of "
                            "%s or Collection" % (string_type.__name__,))

        result = self.command("validate", _unicode(name),
                              scandata=scandata, full=full, session=session)

        valid = True
        # Pre 1.9 results
        if "result" in result:
            info = result["result"]
            if info.find("exception") != -1 or info.find("corrupt") != -1:
                raise CollectionInvalid("%s invalid: %s" % (name, info))
        # Sharded results
        elif "raw" in result:
            for _, res in iteritems(result["raw"]):
                if "result" in res:
                    info = res["result"]
                    if (info.find("exception") != -1 or
                            info.find("corrupt") != -1):
                        raise CollectionInvalid("%s invalid: "
                                                "%s" % (name, info))
                elif not res.get("valid", False):
                    valid = False
                    break
        # Post 1.9 non-sharded results.
        elif not result.get("valid", False):
            valid = False

        if not valid:
            raise CollectionInvalid("%s invalid: %r" % (name, result))

        return result

    def _current_op(self, include_all=False, session=None):
        """Helper for running $currentOp."""
        cmd = SON([("currentOp", 1), ("$all", include_all)])
        with self.__client._socket_for_writes(session) as sock_info:
            if sock_info.max_wire_version >= 4:
                return self.__client.admin._command(
                    sock_info, cmd, codec_options=self.codec_options,
                    session=session)
            else:
                spec = {"$all": True} if include_all else {}
                return _first_batch(sock_info, "admin", "$cmd.sys.inprog",
                                    spec, -1, True, self.codec_options,
                                    ReadPreference.PRIMARY, cmd,
                                    self.client._event_listeners)

    def current_op(self, include_all=False, session=None):
        """**DEPRECATED**: Get information on operations currently running.

        Starting with MongoDB 3.6 this helper is obsolete. The functionality
        provided by this helper is available in MongoDB 3.6+ using the
        `$currentOp aggregation pipeline stage`_, which can be used with
        :meth:`aggregate`. Note that, while this helper can only return
        a single document limited to a 16MB result, :meth:`aggregate`
        returns a cursor avoiding that limitation.

        Users of MongoDB versions older than 3.6 can use the `currentOp command`_
        directly::

          # MongoDB 3.2 and 3.4
          client.admin.command("currentOp")

        Or query the "inprog" virtual collection::

          # MongoDB 2.6 and 3.0
          client.admin["$cmd.sys.inprog"].find_one()

        :Parameters:
          - `include_all` (optional): if ``True`` also list currently
            idle operations in the result
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        .. versionchanged:: 3.9
           Deprecated.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. _$currentOp aggregation pipeline stage: https://docs.mongodb.com/manual/reference/operator/aggregation/currentOp/
        .. _currentOp command: https://docs.mongodb.com/manual/reference/command/currentOp/
        """
        warnings.warn("current_op() is deprecated. See the documentation for "
                      "more information",
                      DeprecationWarning, stacklevel=2)
        return self._current_op(include_all, session)

    def profiling_level(self, session=None):
        """Get the database's current profiling level.

        Returns one of (:data:`~pymongo.OFF`,
        :data:`~pymongo.SLOW_ONLY`, :data:`~pymongo.ALL`).

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. mongodoc:: profiling
        """
        result = self.command("profile", -1, session=session)

        assert result["was"] >= 0 and result["was"] <= 2
        return result["was"]

    def set_profiling_level(self, level, slow_ms=None, session=None):
        """Set the database's profiling level.

        :Parameters:
          - `level`: Specifies a profiling level, see list of possible values
            below.
          - `slow_ms`: Optionally modify the threshold for the profile to
            consider a query or operation.  Even if the profiler is off queries
            slower than the `slow_ms` level will get written to the logs.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        Possible `level` values:

        +----------------------------+------------------------------------+
        | Level                      | Setting                            |
        +============================+====================================+
        | :data:`~pymongo.OFF`       | Off. No profiling.                 |
        +----------------------------+------------------------------------+
        | :data:`~pymongo.SLOW_ONLY` | On. Only includes slow operations. |
        +----------------------------+------------------------------------+
        | :data:`~pymongo.ALL`       | On. Includes all operations.       |
        +----------------------------+------------------------------------+

        Raises :class:`ValueError` if level is not one of
        (:data:`~pymongo.OFF`, :data:`~pymongo.SLOW_ONLY`,
        :data:`~pymongo.ALL`).

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. mongodoc:: profiling
        """
        if not isinstance(level, int) or level < 0 or level > 2:
            raise ValueError("level must be one of (OFF, SLOW_ONLY, ALL)")

        if slow_ms is not None and not isinstance(slow_ms, int):
            raise TypeError("slow_ms must be an integer")

        if slow_ms is not None:
            self.command("profile", level, slowms=slow_ms, session=session)
        else:
            self.command("profile", level, session=session)

    def profiling_info(self, session=None):
        """Returns a list containing current profiling information.

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. mongodoc:: profiling
        """
        return list(self["system.profile"].find(session=session))

    def error(self):
        """**DEPRECATED**: Get the error if one occurred on the last operation.

        This method is obsolete: all MongoDB write operations (insert, update,
        remove, and so on) use the write concern ``w=1`` and report their
        errors by default.

        .. versionchanged:: 2.8
           Deprecated.
        """
        warnings.warn("Database.error() is deprecated",
                      DeprecationWarning, stacklevel=2)

        error = self.command("getlasterror")
        error_msg = error.get("err", "")
        if error_msg is None:
            return None
        if error_msg.startswith("not master"):
            # Reset primary server and request check, if another thread isn't
            # doing so already.
            primary = self.__client.primary
            if primary:
                self.__client._reset_server_and_request_check(primary)
        return error

    def last_status(self):
        """**DEPRECATED**: Get status information from the last operation.

        This method is obsolete: all MongoDB write operations (insert, update,
        remove, and so on) use the write concern ``w=1`` and report their
        errors by default.

        Returns a SON object with status information.

        .. versionchanged:: 2.8
           Deprecated.
        """
        warnings.warn("last_status() is deprecated",
                      DeprecationWarning, stacklevel=2)

        return self.command("getlasterror")

    def previous_error(self):
        """**DEPRECATED**: Get the most recent error on this database.

        This method is obsolete: all MongoDB write operations (insert, update,
        remove, and so on) use the write concern ``w=1`` and report their
        errors by default.

        Only returns errors that have occurred since the last call to
        :meth:`reset_error_history`. Returns None if no such errors have
        occurred.

        .. versionchanged:: 2.8
           Deprecated.
        """
        warnings.warn("previous_error() is deprecated",
                      DeprecationWarning, stacklevel=2)

        error = self.command("getpreverror")
        if error.get("err", 0) is None:
            return None
        return error

    def reset_error_history(self):
        """**DEPRECATED**: Reset the error history of this database.

        This method is obsolete: all MongoDB write operations (insert, update,
        remove, and so on) use the write concern ``w=1`` and report their
        errors by default.

        Calls to :meth:`previous_error` will only return errors that have
        occurred since the most recent call to this method.

        .. versionchanged:: 2.8
           Deprecated.
        """
        warnings.warn("reset_error_history() is deprecated",
                      DeprecationWarning, stacklevel=2)

        self.command("reseterror")

    def __iter__(self):
        return self

    def __next__(self):
        raise TypeError("'Database' object is not iterable")

    next = __next__

    def _default_role(self, read_only):
        """Return the default user role for this database."""
        if self.name == "admin":
            if read_only:
                return "readAnyDatabase"
            else:
                return "root"
        else:
            if read_only:
                return "read"
            else:
                return "dbOwner"

    def _create_or_update_user(
            self, create, name, password, read_only, session=None, **kwargs):
        """Use a command to create (if create=True) or modify a user.
        """
        opts = {}
        if read_only or (create and "roles" not in kwargs):
            warnings.warn("Creating a user with the read_only option "
                          "or without roles is deprecated in MongoDB "
                          ">= 2.6", DeprecationWarning)

            opts["roles"] = [self._default_role(read_only)]

        if read_only:
            warnings.warn("The read_only option is deprecated in MongoDB "
                          ">= 2.6, use 'roles' instead", DeprecationWarning)

        if password is not None:
            if "digestPassword" in kwargs:
                raise ConfigurationError("The digestPassword option is not "
                                         "supported via add_user. Please use "
                                         "db.command('createUser', ...) "
                                         "instead for this option.")
            opts["pwd"] = password

        # Don't send {} as writeConcern.
        if self.write_concern.acknowledged and self.write_concern.document:
            opts["writeConcern"] = self.write_concern.document
        opts.update(kwargs)

        if create:
            command_name = "createUser"
        else:
            command_name = "updateUser"

        self.command(command_name, name, session=session, **opts)

    def add_user(self, name, password=None, read_only=None, session=None,
                 **kwargs):
        """**DEPRECATED**: Create user `name` with password `password`.

        Add a new user with permissions for this :class:`Database`.

        .. note:: Will change the password if user `name` already exists.

        .. note:: add_user is deprecated and will be removed in PyMongo
          4.0. Starting with MongoDB 2.6 user management is handled with four
          database commands, createUser_, usersInfo_, updateUser_, and
          dropUser_.

          To create a user::

            db.command("createUser", "admin", pwd="password", roles=["root"])

          To create a read-only user::

            db.command("createUser", "user", pwd="password", roles=["read"])

          To change a password::

            db.command("updateUser", "user", pwd="newpassword")

          Or change roles::

            db.command("updateUser", "user", roles=["readWrite"])

        .. _createUser: https://docs.mongodb.com/manual/reference/command/createUser/
        .. _usersInfo: https://docs.mongodb.com/manual/reference/command/usersInfo/
        .. _updateUser: https://docs.mongodb.com/manual/reference/command/updateUser/
        .. _dropUser: https://docs.mongodb.com/manual/reference/command/createUser/

        .. warning:: Never create or modify users over an insecure network without
          the use of TLS. See :doc:`/examples/tls` for more information.

        :Parameters:
          - `name`: the name of the user to create
          - `password` (optional): the password of the user to create. Can not
            be used with the ``userSource`` argument.
          - `read_only` (optional): if ``True`` the user will be read only
          - `**kwargs` (optional): optional fields for the user document
            (e.g. ``userSource``, ``otherDBRoles``, or ``roles``). See
            `<http://docs.mongodb.org/manual/reference/privilege-documents>`_
            for more information.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        .. versionchanged:: 3.7
           Added support for SCRAM-SHA-256 users with MongoDB 4.0 and later.

        .. versionchanged:: 3.6
           Added ``session`` parameter. Deprecated add_user.

        .. versionchanged:: 2.5
           Added kwargs support for optional fields introduced in MongoDB 2.4

        .. versionchanged:: 2.2
           Added support for read only users
        """
        warnings.warn("add_user is deprecated and will be removed in PyMongo "
                      "4.0. Use db.command with createUser or updateUser "
                      "instead", DeprecationWarning, stacklevel=2)
        if not isinstance(name, string_type):
            raise TypeError("name must be an "
                            "instance of %s" % (string_type.__name__,))
        if password is not None:
            if not isinstance(password, string_type):
                raise TypeError("password must be an "
                                "instance of %s" % (string_type.__name__,))
            if len(password) == 0:
                raise ValueError("password can't be empty")
        if read_only is not None:
            read_only = common.validate_boolean('read_only', read_only)
            if 'roles' in kwargs:
                raise ConfigurationError("Can not use "
                                         "read_only and roles together")

        try:
            uinfo = self.command("usersInfo", name, session=session)
            # Create the user if not found in uinfo, otherwise update one.
            self._create_or_update_user(
                (not uinfo["users"]), name, password, read_only,
                session=session, **kwargs)
        except OperationFailure as exc:
            # Unauthorized. Attempt to create the user in case of
            # localhost exception.
            if exc.code == 13:
                self._create_or_update_user(
                    True, name, password, read_only, session=session, **kwargs)
            else:
                raise

    def remove_user(self, name, session=None):
        """**DEPRECATED**: Remove user `name` from this :class:`Database`.

        User `name` will no longer have permissions to access this
        :class:`Database`.

        .. note:: remove_user is deprecated and will be removed in PyMongo
          4.0. Use the dropUser command instead::

            db.command("dropUser", "user")

        :Parameters:
          - `name`: the name of the user to remove
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        .. versionchanged:: 3.6
           Added ``session`` parameter. Deprecated remove_user.
        """
        warnings.warn("remove_user is deprecated and will be removed in "
                      "PyMongo 4.0. Use db.command with dropUser "
                      "instead", DeprecationWarning, stacklevel=2)
        cmd = SON([("dropUser", name)])
        # Don't send {} as writeConcern.
        if self.write_concern.acknowledged and self.write_concern.document:
            cmd["writeConcern"] = self.write_concern.document
        self.command(cmd, session=session)

    def authenticate(self, name=None, password=None,
                     source=None, mechanism='DEFAULT', **kwargs):
        """**DEPRECATED**: Authenticate to use this database.

        .. warning:: Starting in MongoDB 3.6, calling :meth:`authenticate`
          invalidates all existing cursors. It may also leave logical sessions
          open on the server for up to 30 minutes until they time out.

        Authentication lasts for the life of the underlying client
        instance, or until :meth:`logout` is called.

        Raises :class:`TypeError` if (required) `name`, (optional) `password`,
        or (optional) `source` is not an instance of :class:`basestring`
        (:class:`str` in python 3).

        .. note::
          - This method authenticates the current connection, and
            will also cause all new :class:`~socket.socket` connections
            in the underlying client instance to be authenticated automatically.

          - Authenticating more than once on the same database with different
            credentials is not supported. You must call :meth:`logout` before
            authenticating with new credentials.

          - When sharing a client instance between multiple threads, all
            threads will share the authentication. If you need different
            authentication profiles for different purposes you must use
            distinct client instances.

        :Parameters:
          - `name`: the name of the user to authenticate. Optional when
            `mechanism` is MONGODB-X509 and the MongoDB server version is
            >= 3.4.
          - `password` (optional): the password of the user to authenticate.
            Not used with GSSAPI or MONGODB-X509 authentication.
          - `source` (optional): the database to authenticate on. If not
            specified the current database is used.
          - `mechanism` (optional): See :data:`~pymongo.auth.MECHANISMS` for
            options. If no mechanism is specified, PyMongo automatically uses
            MONGODB-CR when connected to a pre-3.0 version of MongoDB,
            SCRAM-SHA-1 when connected to MongoDB 3.0 through 3.6, and
            negotiates the mechanism to use (SCRAM-SHA-1 or SCRAM-SHA-256) when
            connected to MongoDB 4.0+.
          - `authMechanismProperties` (optional): Used to specify
            authentication mechanism specific options. To specify the service
            name for GSSAPI authentication pass
            authMechanismProperties='SERVICE_NAME:<service name>'

        .. versionchanged:: 3.7
           Added support for SCRAM-SHA-256 with MongoDB 4.0 and later.

        .. versionchanged:: 3.5
           Deprecated. Authenticating multiple users conflicts with support for
           logical sessions in MongoDB 3.6. To authenticate as multiple users,
           create multiple instances of MongoClient.

        .. versionadded:: 2.8
           Use SCRAM-SHA-1 with MongoDB 3.0 and later.

        .. versionchanged:: 2.5
           Added the `source` and `mechanism` parameters. :meth:`authenticate`
           now raises a subclass of :class:`~pymongo.errors.PyMongoError` if
           authentication fails due to invalid credentials or configuration
           issues.

        .. mongodoc:: authenticate
        """
        if name is not None and not isinstance(name, string_type):
            raise TypeError("name must be an "
                            "instance of %s" % (string_type.__name__,))
        if password is not None and not isinstance(password, string_type):
            raise TypeError("password must be an "
                            "instance of %s" % (string_type.__name__,))
        if source is not None and not isinstance(source, string_type):
            raise TypeError("source must be an "
                            "instance of %s" % (string_type.__name__,))
        common.validate_auth_mechanism('mechanism', mechanism)

        validated_options = {}
        for option, value in iteritems(kwargs):
            normalized, val = common.validate_auth_option(option, value)
            validated_options[normalized] = val

        credentials = auth._build_credentials_tuple(
            mechanism,
            source,
            name,
            password,
            validated_options,
            self.name)

        self.client._cache_credentials(
            self.name,
            credentials,
            connect=True)

        return True

    def logout(self):
        """**DEPRECATED**: Deauthorize use of this database.

        .. warning:: Starting in MongoDB 3.6, calling :meth:`logout`
          invalidates all existing cursors. It may also leave logical sessions
          open on the server for up to 30 minutes until they time out.
        """
        warnings.warn("Database.logout() is deprecated",
                      DeprecationWarning, stacklevel=2)

        # Sockets will be deauthenticated as they are used.
        self.client._purge_credentials(self.name)

    def dereference(self, dbref, session=None, **kwargs):
        """Dereference a :class:`~bson.dbref.DBRef`, getting the
        document it points to.

        Raises :class:`TypeError` if `dbref` is not an instance of
        :class:`~bson.dbref.DBRef`. Returns a document, or ``None`` if
        the reference does not point to a valid document.  Raises
        :class:`ValueError` if `dbref` has a database specified that
        is different from the current database.

        :Parameters:
          - `dbref`: the reference
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): any additional keyword arguments
            are the same as the arguments to
            :meth:`~pymongo.collection.Collection.find`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        if not isinstance(dbref, DBRef):
            raise TypeError("cannot dereference a %s" % type(dbref))
        if dbref.database is not None and dbref.database != self.__name:
            raise ValueError("trying to dereference a DBRef that points to "
                             "another database (%r not %r)" % (dbref.database,
                                                               self.__name))
        return self[dbref.collection].find_one(
            {"_id": dbref.id}, session=session, **kwargs)

    def eval(self, code, *args):
        """**DEPRECATED**: Evaluate a JavaScript expression in MongoDB.

        :Parameters:
          - `code`: string representation of JavaScript code to be
            evaluated
          - `args` (optional): additional positional arguments are
            passed to the `code` being evaluated

        .. warning:: the eval command is deprecated in MongoDB 3.0 and
          will be removed in a future server version.
        """
        warnings.warn("Database.eval() is deprecated",
                      DeprecationWarning, stacklevel=2)

        if not isinstance(code, Code):
            code = Code(code)

        result = self.command("$eval", code, args=args)
        return result.get("retval", None)

    def __call__(self, *args, **kwargs):
        """This is only here so that some API misusages are easier to debug.
        """
        raise TypeError("'Database' object is not callable. If you meant to "
                        "call the '%s' method on a '%s' object it is "
                        "failing because no such method exists." % (
                            self.__name, self.__client.__class__.__name__))


class SystemJS(object):
    """**DEPRECATED**: Helper class for dealing with stored JavaScript.
    """

    def __init__(self, database):
        """**DEPRECATED**: Get a system js helper for the database `database`.

        SystemJS will be removed in PyMongo 4.0.
        """
        warnings.warn("SystemJS is deprecated",
                      DeprecationWarning, stacklevel=2)

        if not database.write_concern.acknowledged:
            database = database.client.get_database(
                database.name, write_concern=DEFAULT_WRITE_CONCERN)
        # can't just assign it since we've overridden __setattr__
        object.__setattr__(self, "_db", database)

    def __setattr__(self, name, code):
        self._db.system.js.replace_one(
            {"_id": name}, {"_id": name, "value": Code(code)}, True)

    def __setitem__(self, name, code):
        self.__setattr__(name, code)

    def __delattr__(self, name):
        self._db.system.js.delete_one({"_id": name})

    def __delitem__(self, name):
        self.__delattr__(name)

    def __getattr__(self, name):
        return lambda *args: self._db.eval(Code("function() { "
                                                "return this[name].apply("
                                                "this, arguments); }",
                                                scope={'name': name}), *args)

    def __getitem__(self, name):
        return self.__getattr__(name)

    def list(self):
        """Get a list of the names of the functions stored in this database."""
        return [x["_id"] for x in self._db.system.js.find(projection=["_id"])]
