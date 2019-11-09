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

"""Collection level utilities for Mongo."""

import datetime
import warnings

from bson.code import Code
from bson.objectid import ObjectId
from bson.py3compat import (_unicode,
                            abc,
                            integer_types,
                            string_type)
from bson.raw_bson import RawBSONDocument
from bson.codec_options import CodecOptions
from bson.son import SON
from pymongo import (common,
                     helpers,
                     message)
from pymongo.aggregation import (_CollectionAggregationCommand,
                                 _CollectionRawAggregationCommand)
from pymongo.bulk import BulkOperationBuilder, _Bulk
from pymongo.command_cursor import CommandCursor, RawBatchCommandCursor
from pymongo.common import ORDERED_TYPES
from pymongo.collation import validate_collation_or_none
from pymongo.change_stream import CollectionChangeStream
from pymongo.cursor import Cursor, RawBatchCursor
from pymongo.errors import (BulkWriteError,
                            ConfigurationError,
                            InvalidName,
                            InvalidOperation,
                            OperationFailure)
from pymongo.helpers import (_check_write_command_response,
                             _raise_last_error)
from pymongo.message import _UNICODE_REPLACE_CODEC_OPTIONS
from pymongo.operations import IndexModel
from pymongo.read_preferences import ReadPreference
from pymongo.results import (BulkWriteResult,
                             DeleteResult,
                             InsertOneResult,
                             InsertManyResult,
                             UpdateResult)
from pymongo.write_concern import WriteConcern

_NO_OBJ_ERROR = "No matching object found"
_UJOIN = u"%s.%s"
_FIND_AND_MODIFY_DOC_FIELDS = {'value': 1}


class ReturnDocument(object):
    """An enum used with
    :meth:`~pymongo.collection.Collection.find_one_and_replace` and
    :meth:`~pymongo.collection.Collection.find_one_and_update`.
    """
    BEFORE = False
    """Return the original document before it was updated/replaced, or
    ``None`` if no document matches the query.
    """
    AFTER = True
    """Return the updated/replaced or inserted document."""


class Collection(common.BaseObject):
    """A Mongo collection.
    """

    def __init__(self, database, name, create=False, codec_options=None,
                 read_preference=None, write_concern=None, read_concern=None,
                 session=None, **kwargs):
        """Get / create a Mongo collection.

        Raises :class:`TypeError` if `name` is not an instance of
        :class:`basestring` (:class:`str` in python 3). Raises
        :class:`~pymongo.errors.InvalidName` if `name` is not a valid
        collection name. Any additional keyword arguments will be used
        as options passed to the create command. See
        :meth:`~pymongo.database.Database.create_collection` for valid
        options.

        If `create` is ``True``, `collation` is specified, or any additional
        keyword arguments are present, a ``create`` command will be
        sent, using ``session`` if specified. Otherwise, a ``create`` command
        will not be sent and the collection will be created implicitly on first
        use. The optional ``session`` argument is *only* used for the ``create``
        command, it is not associated with the collection afterward.

        :Parameters:
          - `database`: the database to get a collection from
          - `name`: the name of the collection to get
          - `create` (optional): if ``True``, force collection
            creation even without options being set
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) database.codec_options is used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) database.read_preference is used.
          - `write_concern` (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) database.write_concern is used.
          - `read_concern` (optional): An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) database.read_concern is used.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. If a collation is provided,
            it will be passed to the create collection command. This option is
            only supported on MongoDB 3.4 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession` that is used with
            the create collection command
          - `**kwargs` (optional): additional keyword arguments will
            be passed as options for the create collection command

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Support the `collation` option.

        .. versionchanged:: 3.2
           Added the read_concern option.

        .. versionchanged:: 3.0
           Added the codec_options, read_preference, and write_concern options.
           Removed the uuid_subtype attribute.
           :class:`~pymongo.collection.Collection` no longer returns an
           instance of :class:`~pymongo.collection.Collection` for attribute
           names with leading underscores. You must use dict-style lookups
           instead::

               collection['__my_collection__']

           Not:

               collection.__my_collection__

        .. versionchanged:: 2.2
           Removed deprecated argument: options

        .. versionadded:: 2.1
           uuid_subtype attribute

        .. mongodoc:: collections
        """
        super(Collection, self).__init__(
            codec_options or database.codec_options,
            read_preference or database.read_preference,
            write_concern or database.write_concern,
            read_concern or database.read_concern)

        if not isinstance(name, string_type):
            raise TypeError("name must be an instance "
                            "of %s" % (string_type.__name__,))

        if not name or ".." in name:
            raise InvalidName("collection names cannot be empty")
        if "$" in name and not (name.startswith("oplog.$main") or
                                name.startswith("$cmd")):
            raise InvalidName("collection names must not "
                              "contain '$': %r" % name)
        if name[0] == "." or name[-1] == ".":
            raise InvalidName("collection names must not start "
                              "or end with '.': %r" % name)
        if "\x00" in name:
            raise InvalidName("collection names must not contain the "
                              "null character")
        collation = validate_collation_or_none(kwargs.pop('collation', None))

        self.__database = database
        self.__name = _unicode(name)
        self.__full_name = _UJOIN % (self.__database.name, self.__name)
        if create or kwargs or collation:
            self.__create(kwargs, collation, session)

        self.__write_response_codec_options = self.codec_options._replace(
            unicode_decode_error_handler='replace',
            document_class=dict)

    def _socket_for_reads(self, session):
        return self.__database.client._socket_for_reads(
            self._read_preference_for(session), session)

    def _socket_for_writes(self, session):
        return self.__database.client._socket_for_writes(session)

    def _command(self, sock_info, command, slave_ok=False,
                 read_preference=None,
                 codec_options=None, check=True, allowable_errors=None,
                 read_concern=None,
                 write_concern=None,
                 collation=None,
                 session=None,
                 retryable_write=False,
                 user_fields=None):
        """Internal command helper.

        :Parameters:
          - `sock_info` - A SocketInfo instance.
          - `command` - The command itself, as a SON instance.
          - `slave_ok`: whether to set the SlaveOkay wire protocol bit.
          - `codec_options` (optional) - An instance of
            :class:`~bson.codec_options.CodecOptions`.
          - `check`: raise OperationFailure if there are errors
          - `allowable_errors`: errors to ignore if `check` is True
          - `read_concern` (optional) - An instance of
            :class:`~pymongo.read_concern.ReadConcern`.
          - `write_concern`: An instance of
            :class:`~pymongo.write_concern.WriteConcern`. This option is only
            valid for MongoDB 3.4 and above.
          - `collation` (optional) - An instance of
            :class:`~pymongo.collation.Collation`.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `retryable_write` (optional): True if this command is a retryable
            write.
          - `user_fields` (optional): Response fields that should be decoded
            using the TypeDecoders from codec_options, passed to
            bson._decode_all_selective.

        :Returns:
          The result document.
        """
        with self.__database.client._tmp_session(session) as s:
            return sock_info.command(
                self.__database.name,
                command,
                slave_ok,
                read_preference or self._read_preference_for(session),
                codec_options or self.codec_options,
                check,
                allowable_errors,
                read_concern=read_concern,
                write_concern=write_concern,
                parse_write_concern_error=True,
                collation=collation,
                session=s,
                client=self.__database.client,
                retryable_write=retryable_write,
                user_fields=user_fields)

    def __create(self, options, collation, session):
        """Sends a create command with the given options.
        """
        cmd = SON([("create", self.__name)])
        if options:
            if "size" in options:
                options["size"] = float(options["size"])
            cmd.update(options)
        with self._socket_for_writes(session) as sock_info:
            self._command(
                sock_info, cmd, read_preference=ReadPreference.PRIMARY,
                write_concern=self._write_concern_for(session),
                collation=collation, session=session)

    def __getattr__(self, name):
        """Get a sub-collection of this collection by name.

        Raises InvalidName if an invalid collection name is used.

        :Parameters:
          - `name`: the name of the collection to get
        """
        if name.startswith('_'):
            full_name = _UJOIN % (self.__name, name)
            raise AttributeError(
                "Collection has no attribute %r. To access the %s"
                " collection, use database['%s']." % (
                    name, full_name, full_name))
        return self.__getitem__(name)

    def __getitem__(self, name):
        return Collection(self.__database,
                          _UJOIN % (self.__name, name),
                          False,
                          self.codec_options,
                          self.read_preference,
                          self.write_concern,
                          self.read_concern)

    def __repr__(self):
        return "Collection(%r, %r)" % (self.__database, self.__name)

    def __eq__(self, other):
        if isinstance(other, Collection):
            return (self.__database == other.database and
                    self.__name == other.name)
        return NotImplemented

    def __ne__(self, other):
        return not self == other

    @property
    def full_name(self):
        """The full name of this :class:`Collection`.

        The full name is of the form `database_name.collection_name`.
        """
        return self.__full_name

    @property
    def name(self):
        """The name of this :class:`Collection`."""
        return self.__name

    @property
    def database(self):
        """The :class:`~pymongo.database.Database` that this
        :class:`Collection` is a part of.
        """
        return self.__database

    def with_options(self, codec_options=None, read_preference=None,
                     write_concern=None, read_concern=None):
        """Get a clone of this collection changing the specified settings.

          >>> coll1.read_preference
          Primary()
          >>> from pymongo import ReadPreference
          >>> coll2 = coll1.with_options(read_preference=ReadPreference.SECONDARY)
          >>> coll1.read_preference
          Primary()
          >>> coll2.read_preference
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
        """
        return Collection(self.__database,
                          self.__name,
                          False,
                          codec_options or self.codec_options,
                          read_preference or self.read_preference,
                          write_concern or self.write_concern,
                          read_concern or self.read_concern)

    def initialize_unordered_bulk_op(self, bypass_document_validation=False):
        """**DEPRECATED** - Initialize an unordered batch of write operations.

        Operations will be performed on the server in arbitrary order,
        possibly in parallel. All operations will be attempted.

        :Parameters:
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.

        Returns a :class:`~pymongo.bulk.BulkOperationBuilder` instance.

        See :ref:`unordered_bulk` for examples.

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 3.5
           Deprecated. Use :meth:`~pymongo.collection.Collection.bulk_write`
           instead.

        .. versionchanged:: 3.2
           Added bypass_document_validation support

        .. versionadded:: 2.7
        """
        warnings.warn("initialize_unordered_bulk_op is deprecated",
                      DeprecationWarning, stacklevel=2)
        return BulkOperationBuilder(self, False, bypass_document_validation)

    def initialize_ordered_bulk_op(self, bypass_document_validation=False):
        """**DEPRECATED** - Initialize an ordered batch of write operations.

        Operations will be performed on the server serially, in the
        order provided. If an error occurs all remaining operations
        are aborted.

        :Parameters:
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.

        Returns a :class:`~pymongo.bulk.BulkOperationBuilder` instance.

        See :ref:`ordered_bulk` for examples.

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 3.5
           Deprecated. Use :meth:`~pymongo.collection.Collection.bulk_write`
           instead.

        .. versionchanged:: 3.2
           Added bypass_document_validation support

        .. versionadded:: 2.7
        """
        warnings.warn("initialize_ordered_bulk_op is deprecated",
                      DeprecationWarning, stacklevel=2)
        return BulkOperationBuilder(self, True, bypass_document_validation)

    def bulk_write(self, requests, ordered=True,
                   bypass_document_validation=False, session=None):
        """Send a batch of write operations to the server.

        Requests are passed as a list of write operation instances (
        :class:`~pymongo.operations.InsertOne`,
        :class:`~pymongo.operations.UpdateOne`,
        :class:`~pymongo.operations.UpdateMany`,
        :class:`~pymongo.operations.ReplaceOne`,
        :class:`~pymongo.operations.DeleteOne`, or
        :class:`~pymongo.operations.DeleteMany`).

          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {u'x': 1, u'_id': ObjectId('54f62e60fba5226811f634ef')}
          {u'x': 1, u'_id': ObjectId('54f62e60fba5226811f634f0')}
          >>> # DeleteMany, UpdateOne, and UpdateMany are also available.
          ...
          >>> from pymongo import InsertOne, DeleteOne, ReplaceOne
          >>> requests = [InsertOne({'y': 1}), DeleteOne({'x': 1}),
          ...             ReplaceOne({'w': 1}, {'z': 1}, upsert=True)]
          >>> result = db.test.bulk_write(requests)
          >>> result.inserted_count
          1
          >>> result.deleted_count
          1
          >>> result.modified_count
          0
          >>> result.upserted_ids
          {2: ObjectId('54f62ee28891e756a6e1abd5')}
          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {u'x': 1, u'_id': ObjectId('54f62e60fba5226811f634f0')}
          {u'y': 1, u'_id': ObjectId('54f62ee2fba5226811f634f1')}
          {u'z': 1, u'_id': ObjectId('54f62ee28891e756a6e1abd5')}

        :Parameters:
          - `requests`: A list of write operations (see examples above).
          - `ordered` (optional): If ``True`` (the default) requests will be
            performed on the server serially, in the order provided. If an error
            occurs all remaining operations are aborted. If ``False`` requests
            will be performed on the server in arbitrary order, possibly in
            parallel, and all operations will be attempted.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        :Returns:
          An instance of :class:`~pymongo.results.BulkWriteResult`.

        .. seealso:: :ref:`writes-and-ids`

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.2
          Added bypass_document_validation support

        .. versionadded:: 3.0
        """
        common.validate_list("requests", requests)

        blk = _Bulk(self, ordered, bypass_document_validation)
        for request in requests:
            try:
                request._add_to_bulk(blk)
            except AttributeError:
                raise TypeError("%r is not a valid request" % (request,))

        write_concern = self._write_concern_for(session)
        bulk_api_result = blk.execute(write_concern, session)
        if bulk_api_result is not None:
            return BulkWriteResult(bulk_api_result, True)
        return BulkWriteResult({}, False)

    def _legacy_write(self, sock_info, name, cmd, op_id,
                      bypass_doc_val, func, *args):
        """Internal legacy unacknowledged write helper."""
        # Cannot have both unacknowledged write and bypass document validation.
        if bypass_doc_val and sock_info.max_wire_version >= 4:
            raise OperationFailure("Cannot set bypass_document_validation with"
                                   " unacknowledged write concern")
        listeners = self.database.client._event_listeners
        publish = listeners.enabled_for_commands

        if publish:
            start = datetime.datetime.now()
        args = args + (sock_info.compression_context,)
        rqst_id, msg, max_size = func(*args)
        if publish:
            duration = datetime.datetime.now() - start
            listeners.publish_command_start(
                cmd, self.__database.name, rqst_id, sock_info.address, op_id)
            start = datetime.datetime.now()
        try:
            result = sock_info.legacy_write(rqst_id, msg, max_size, False)
        except Exception as exc:
            if publish:
                dur = (datetime.datetime.now() - start) + duration
                if isinstance(exc, OperationFailure):
                    details = exc.details
                    # Succeed if GLE was successful and this is a write error.
                    if details.get("ok") and "n" in details:
                        reply = message._convert_write_result(
                            name, cmd, details)
                        listeners.publish_command_success(
                            dur, reply, name, rqst_id, sock_info.address, op_id)
                        raise
                else:
                    details = message._convert_exception(exc)
                listeners.publish_command_failure(
                    dur, details, name, rqst_id, sock_info.address, op_id)
            raise
        if publish:
            if result is not None:
                reply = message._convert_write_result(name, cmd, result)
            else:
                # Comply with APM spec.
                reply = {'ok': 1}
            duration = (datetime.datetime.now() - start) + duration
            listeners.publish_command_success(
                duration, reply, name, rqst_id, sock_info.address, op_id)
        return result

    def _insert_one(
            self, doc, ordered,
            check_keys, manipulate, write_concern, op_id, bypass_doc_val,
            session):
        """Internal helper for inserting a single document."""
        if manipulate:
            doc = self.__database._apply_incoming_manipulators(doc, self)
            if not isinstance(doc, RawBSONDocument) and '_id' not in doc:
                doc['_id'] = ObjectId()
            doc = self.__database._apply_incoming_copying_manipulators(doc,
                                                                       self)
        write_concern = write_concern or self.write_concern
        acknowledged = write_concern.acknowledged
        command = SON([('insert', self.name),
                       ('ordered', ordered),
                       ('documents', [doc])])
        if not write_concern.is_server_default:
            command['writeConcern'] = write_concern.document

        def _insert_command(session, sock_info, retryable_write):
            if not sock_info.op_msg_enabled and not acknowledged:
                # Legacy OP_INSERT.
                return self._legacy_write(
                    sock_info, 'insert', command, op_id,
                    bypass_doc_val, message.insert, self.__full_name,
                    [doc], check_keys, False, write_concern.document, False,
                    self.__write_response_codec_options)

            if bypass_doc_val and sock_info.max_wire_version >= 4:
                command['bypassDocumentValidation'] = True

            result = sock_info.command(
                self.__database.name,
                command,
                write_concern=write_concern,
                codec_options=self.__write_response_codec_options,
                check_keys=check_keys,
                session=session,
                client=self.__database.client,
                retryable_write=retryable_write)

            _check_write_command_response(result)

        self.__database.client._retryable_write(
            acknowledged, _insert_command, session)

        if not isinstance(doc, RawBSONDocument):
            return doc.get('_id')

    def _insert(self, docs, ordered=True, check_keys=True,
                manipulate=False, write_concern=None, op_id=None,
                bypass_doc_val=False, session=None):
        """Internal insert helper."""
        if isinstance(docs, abc.Mapping):
            return self._insert_one(
                docs, ordered, check_keys, manipulate, write_concern, op_id,
                bypass_doc_val, session)

        ids = []

        if manipulate:
            def gen():
                """Generator that applies SON manipulators to each document
                and adds _id if necessary.
                """
                _db = self.__database
                for doc in docs:
                    # Apply user-configured SON manipulators. This order of
                    # operations is required for backwards compatibility,
                    # see PYTHON-709.
                    doc = _db._apply_incoming_manipulators(doc, self)
                    if not (isinstance(doc, RawBSONDocument) or '_id' in doc):
                        doc['_id'] = ObjectId()

                    doc = _db._apply_incoming_copying_manipulators(doc, self)
                    ids.append(doc['_id'])
                    yield doc
        else:
            def gen():
                """Generator that only tracks existing _ids."""
                for doc in docs:
                    # Don't inflate RawBSONDocument by touching fields.
                    if not isinstance(doc, RawBSONDocument):
                        ids.append(doc.get('_id'))
                    yield doc

        write_concern = write_concern or self._write_concern_for(session)
        blk = _Bulk(self, ordered, bypass_doc_val)
        blk.ops = [(message._INSERT, doc) for doc in gen()]
        try:
            blk.execute(write_concern, session=session)
        except BulkWriteError as bwe:
            _raise_last_error(bwe.details)
        return ids

    def insert_one(self, document, bypass_document_validation=False,
                   session=None):
        """Insert a single document.

          >>> db.test.count_documents({'x': 1})
          0
          >>> result = db.test.insert_one({'x': 1})
          >>> result.inserted_id
          ObjectId('54f112defba522406c9cc208')
          >>> db.test.find_one({'x': 1})
          {u'x': 1, u'_id': ObjectId('54f112defba522406c9cc208')}

        :Parameters:
          - `document`: The document to insert. Must be a mutable mapping
            type. If the document does not have an _id field one will be
            added automatically.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        :Returns:
          - An instance of :class:`~pymongo.results.InsertOneResult`.

        .. seealso:: :ref:`writes-and-ids`

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.2
          Added bypass_document_validation support

        .. versionadded:: 3.0
        """
        common.validate_is_document_type("document", document)
        if not (isinstance(document, RawBSONDocument) or "_id" in document):
            document["_id"] = ObjectId()

        write_concern = self._write_concern_for(session)
        return InsertOneResult(
            self._insert(document,
                         write_concern=write_concern,
                         bypass_doc_val=bypass_document_validation,
                         session=session),
            write_concern.acknowledged)

    def insert_many(self, documents, ordered=True,
                    bypass_document_validation=False, session=None):
        """Insert an iterable of documents.

          >>> db.test.count_documents({})
          0
          >>> result = db.test.insert_many([{'x': i} for i in range(2)])
          >>> result.inserted_ids
          [ObjectId('54f113fffba522406c9cc20e'), ObjectId('54f113fffba522406c9cc20f')]
          >>> db.test.count_documents({})
          2

        :Parameters:
          - `documents`: A iterable of documents to insert.
          - `ordered` (optional): If ``True`` (the default) documents will be
            inserted on the server serially, in the order provided. If an error
            occurs all remaining inserts are aborted. If ``False``, documents
            will be inserted on the server in arbitrary order, possibly in
            parallel, and all document inserts will be attempted.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        :Returns:
          An instance of :class:`~pymongo.results.InsertManyResult`.

        .. seealso:: :ref:`writes-and-ids`

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.2
          Added bypass_document_validation support

        .. versionadded:: 3.0
        """
        if not isinstance(documents, abc.Iterable) or not documents:
            raise TypeError("documents must be a non-empty list")
        inserted_ids = []
        def gen():
            """A generator that validates documents and handles _ids."""
            for document in documents:
                common.validate_is_document_type("document", document)
                if not isinstance(document, RawBSONDocument):
                    if "_id" not in document:
                        document["_id"] = ObjectId()
                    inserted_ids.append(document["_id"])
                yield (message._INSERT, document)

        write_concern = self._write_concern_for(session)
        blk = _Bulk(self, ordered, bypass_document_validation)
        blk.ops = [doc for doc in gen()]
        blk.execute(write_concern, session=session)
        return InsertManyResult(inserted_ids, write_concern.acknowledged)

    def _update(self, sock_info, criteria, document, upsert=False,
                check_keys=True, multi=False, manipulate=False,
                write_concern=None, op_id=None, ordered=True,
                bypass_doc_val=False, collation=None, array_filters=None,
                session=None, retryable_write=False):
        """Internal update / replace helper."""
        common.validate_boolean("upsert", upsert)
        if manipulate:
            document = self.__database._fix_incoming(document, self)
        collation = validate_collation_or_none(collation)
        write_concern = write_concern or self.write_concern
        acknowledged = write_concern.acknowledged
        update_doc = SON([('q', criteria),
                          ('u', document),
                          ('multi', multi),
                          ('upsert', upsert)])
        if collation is not None:
            if sock_info.max_wire_version < 5:
                raise ConfigurationError(
                    'Must be connected to MongoDB 3.4+ to use collations.')
            elif not acknowledged:
                raise ConfigurationError(
                    'Collation is unsupported for unacknowledged writes.')
            else:
                update_doc['collation'] = collation
        if array_filters is not None:
            if sock_info.max_wire_version < 6:
                raise ConfigurationError(
                    'Must be connected to MongoDB 3.6+ to use array_filters.')
            elif not acknowledged:
                raise ConfigurationError(
                    'arrayFilters is unsupported for unacknowledged writes.')
            else:
                update_doc['arrayFilters'] = array_filters
        command = SON([('update', self.name),
                       ('ordered', ordered),
                       ('updates', [update_doc])])
        if not write_concern.is_server_default:
            command['writeConcern'] = write_concern.document

        if not sock_info.op_msg_enabled and not acknowledged:
            # Legacy OP_UPDATE.
            return self._legacy_write(
                sock_info, 'update', command, op_id,
                bypass_doc_val, message.update, self.__full_name, upsert,
                multi, criteria, document, False, write_concern.document,
                check_keys, self.__write_response_codec_options)

        # Update command.
        if bypass_doc_val and sock_info.max_wire_version >= 4:
            command['bypassDocumentValidation'] = True

        # The command result has to be published for APM unmodified
        # so we make a shallow copy here before adding updatedExisting.
        result = sock_info.command(
            self.__database.name,
            command,
            write_concern=write_concern,
            codec_options=self.__write_response_codec_options,
            session=session,
            client=self.__database.client,
            retryable_write=retryable_write).copy()
        _check_write_command_response(result)
        # Add the updatedExisting field for compatibility.
        if result.get('n') and 'upserted' not in result:
            result['updatedExisting'] = True
        else:
            result['updatedExisting'] = False
            # MongoDB >= 2.6.0 returns the upsert _id in an array
            # element. Break it out for backward compatibility.
            if 'upserted' in result:
                result['upserted'] = result['upserted'][0]['_id']

        if not acknowledged:
            return None
        return result

    def _update_retryable(
            self, criteria, document, upsert=False,
            check_keys=True, multi=False, manipulate=False,
            write_concern=None, op_id=None, ordered=True,
            bypass_doc_val=False, collation=None, array_filters=None,
            session=None):
        """Internal update / replace helper."""
        def _update(session, sock_info, retryable_write):
            return self._update(
                sock_info, criteria, document, upsert=upsert,
                check_keys=check_keys, multi=multi, manipulate=manipulate,
                write_concern=write_concern, op_id=op_id, ordered=ordered,
                bypass_doc_val=bypass_doc_val, collation=collation,
                array_filters=array_filters, session=session,
                retryable_write=retryable_write)

        return self.__database.client._retryable_write(
            (write_concern or self.write_concern).acknowledged and not multi,
            _update, session)

    def replace_one(self, filter, replacement, upsert=False,
                    bypass_document_validation=False, collation=None,
                    session=None):
        """Replace a single document matching the filter.

          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {u'x': 1, u'_id': ObjectId('54f4c5befba5220aa4d6dee7')}
          >>> result = db.test.replace_one({'x': 1}, {'y': 1})
          >>> result.matched_count
          1
          >>> result.modified_count
          1
          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {u'y': 1, u'_id': ObjectId('54f4c5befba5220aa4d6dee7')}

        The *upsert* option can be used to insert a new document if a matching
        document does not exist.

          >>> result = db.test.replace_one({'x': 1}, {'x': 1}, True)
          >>> result.matched_count
          0
          >>> result.modified_count
          0
          >>> result.upserted_id
          ObjectId('54f11e5c8891e756a6e1abd4')
          >>> db.test.find_one({'x': 1})
          {u'x': 1, u'_id': ObjectId('54f11e5c8891e756a6e1abd4')}

        :Parameters:
          - `filter`: A query that matches the document to replace.
          - `replacement`: The new document.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        :Returns:
          - An instance of :class:`~pymongo.results.UpdateResult`.

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
          Added the `collation` option.

        .. versionchanged:: 3.2
          Added bypass_document_validation support

        .. versionadded:: 3.0
        """
        common.validate_is_mapping("filter", filter)
        common.validate_ok_for_replace(replacement)

        write_concern = self._write_concern_for(session)
        return UpdateResult(
            self._update_retryable(
                filter, replacement, upsert,
                write_concern=write_concern,
                bypass_doc_val=bypass_document_validation,
                collation=collation, session=session),
            write_concern.acknowledged)

    def update_one(self, filter, update, upsert=False,
                   bypass_document_validation=False,
                   collation=None, array_filters=None, session=None):
        """Update a single document matching the filter.

          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {u'x': 1, u'_id': 0}
          {u'x': 1, u'_id': 1}
          {u'x': 1, u'_id': 2}
          >>> result = db.test.update_one({'x': 1}, {'$inc': {'x': 3}})
          >>> result.matched_count
          1
          >>> result.modified_count
          1
          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {u'x': 4, u'_id': 0}
          {u'x': 1, u'_id': 1}
          {u'x': 1, u'_id': 2}

        :Parameters:
          - `filter`: A query that matches the document to update.
          - `update`: The modifications to apply.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.
          - `array_filters` (optional): A list of filters specifying which
            array elements an update should apply. Requires MongoDB 3.6+.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        :Returns:
          - An instance of :class:`~pymongo.results.UpdateResult`.

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 3.9
           Added the ability to accept a pipeline as the `update`.

        .. versionchanged:: 3.6
           Added the `array_filters` and ``session`` parameters.

        .. versionchanged:: 3.4
          Added the `collation` option.

        .. versionchanged:: 3.2
          Added bypass_document_validation support

        .. versionadded:: 3.0
        """
        common.validate_is_mapping("filter", filter)
        common.validate_ok_for_update(update)
        common.validate_list_or_none('array_filters', array_filters)

        write_concern = self._write_concern_for(session)
        return UpdateResult(
            self._update_retryable(
                filter, update, upsert, check_keys=False,
                write_concern=write_concern,
                bypass_doc_val=bypass_document_validation,
                collation=collation, array_filters=array_filters,
                session=session),
            write_concern.acknowledged)

    def update_many(self, filter, update, upsert=False, array_filters=None,
                    bypass_document_validation=False, collation=None,
                    session=None):
        """Update one or more documents that match the filter.

          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {u'x': 1, u'_id': 0}
          {u'x': 1, u'_id': 1}
          {u'x': 1, u'_id': 2}
          >>> result = db.test.update_many({'x': 1}, {'$inc': {'x': 3}})
          >>> result.matched_count
          3
          >>> result.modified_count
          3
          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {u'x': 4, u'_id': 0}
          {u'x': 4, u'_id': 1}
          {u'x': 4, u'_id': 2}

        :Parameters:
          - `filter`: A query that matches the documents to update.
          - `update`: The modifications to apply.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
          - `bypass_document_validation` (optional): If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.
          - `array_filters` (optional): A list of filters specifying which
            array elements an update should apply. Requires MongoDB 3.6+.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        :Returns:
          - An instance of :class:`~pymongo.results.UpdateResult`.

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 3.9
           Added the ability to accept a pipeline as the `update`.

        .. versionchanged:: 3.6
           Added ``array_filters`` and ``session`` parameters.

        .. versionchanged:: 3.4
          Added the `collation` option.

        .. versionchanged:: 3.2
          Added bypass_document_validation support

        .. versionadded:: 3.0
        """
        common.validate_is_mapping("filter", filter)
        common.validate_ok_for_update(update)
        common.validate_list_or_none('array_filters', array_filters)

        write_concern = self._write_concern_for(session)
        return UpdateResult(
            self._update_retryable(
                filter, update, upsert, check_keys=False, multi=True,
                write_concern=write_concern,
                bypass_doc_val=bypass_document_validation,
                collation=collation, array_filters=array_filters,
                session=session),
            write_concern.acknowledged)

    def drop(self, session=None):
        """Alias for :meth:`~pymongo.database.Database.drop_collection`.

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        The following two calls are equivalent:

          >>> db.foo.drop()
          >>> db.drop_collection("foo")

        .. versionchanged:: 3.7
           :meth:`drop` now respects this :class:`Collection`'s :attr:`write_concern`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        dbo = self.__database.client.get_database(
            self.__database.name,
            self.codec_options,
            self.read_preference,
            self.write_concern,
            self.read_concern)
        dbo.drop_collection(self.__name, session=session)

    def _delete(
            self, sock_info, criteria, multi,
            write_concern=None, op_id=None, ordered=True,
            collation=None, session=None, retryable_write=False):
        """Internal delete helper."""
        common.validate_is_mapping("filter", criteria)
        write_concern = write_concern or self.write_concern
        acknowledged = write_concern.acknowledged
        delete_doc = SON([('q', criteria),
                          ('limit', int(not multi))])
        collation = validate_collation_or_none(collation)
        if collation is not None:
            if sock_info.max_wire_version < 5:
                raise ConfigurationError(
                    'Must be connected to MongoDB 3.4+ to use collations.')
            elif not acknowledged:
                raise ConfigurationError(
                    'Collation is unsupported for unacknowledged writes.')
            else:
                delete_doc['collation'] = collation
        command = SON([('delete', self.name),
                       ('ordered', ordered),
                       ('deletes', [delete_doc])])
        if not write_concern.is_server_default:
            command['writeConcern'] = write_concern.document

        if not sock_info.op_msg_enabled and not acknowledged:
            # Legacy OP_DELETE.
            return self._legacy_write(
                sock_info, 'delete', command, op_id,
                False, message.delete, self.__full_name, criteria,
                False, write_concern.document,
                self.__write_response_codec_options,
                int(not multi))
        # Delete command.
        result = sock_info.command(
            self.__database.name,
            command,
            write_concern=write_concern,
            codec_options=self.__write_response_codec_options,
            session=session,
            client=self.__database.client,
            retryable_write=retryable_write)
        _check_write_command_response(result)
        return result

    def _delete_retryable(
            self, criteria, multi,
            write_concern=None, op_id=None, ordered=True,
            collation=None, session=None):
        """Internal delete helper."""
        def _delete(session, sock_info, retryable_write):
            return self._delete(
                sock_info, criteria, multi,
                write_concern=write_concern, op_id=op_id, ordered=ordered,
                collation=collation, session=session,
                retryable_write=retryable_write)

        return self.__database.client._retryable_write(
            (write_concern or self.write_concern).acknowledged and not multi,
            _delete, session)

    def delete_one(self, filter, collation=None, session=None):
        """Delete a single document matching the filter.

          >>> db.test.count_documents({'x': 1})
          3
          >>> result = db.test.delete_one({'x': 1})
          >>> result.deleted_count
          1
          >>> db.test.count_documents({'x': 1})
          2

        :Parameters:
          - `filter`: A query that matches the document to delete.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        :Returns:
          - An instance of :class:`~pymongo.results.DeleteResult`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
          Added the `collation` option.

        .. versionadded:: 3.0
        """
        write_concern = self._write_concern_for(session)
        return DeleteResult(
            self._delete_retryable(
                filter, False,
                write_concern=write_concern,
                collation=collation, session=session),
            write_concern.acknowledged)

    def delete_many(self, filter, collation=None, session=None):
        """Delete one or more documents matching the filter.

          >>> db.test.count_documents({'x': 1})
          3
          >>> result = db.test.delete_many({'x': 1})
          >>> result.deleted_count
          3
          >>> db.test.count_documents({'x': 1})
          0

        :Parameters:
          - `filter`: A query that matches the documents to delete.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        :Returns:
          - An instance of :class:`~pymongo.results.DeleteResult`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
          Added the `collation` option.

        .. versionadded:: 3.0
        """
        write_concern = self._write_concern_for(session)
        return DeleteResult(
            self._delete_retryable(
                filter, True,
                write_concern=write_concern,
                collation=collation, session=session),
            write_concern.acknowledged)

    def find_one(self, filter=None, *args, **kwargs):
        """Get a single document from the database.

        All arguments to :meth:`find` are also valid arguments for
        :meth:`find_one`, although any `limit` argument will be
        ignored. Returns a single document, or ``None`` if no matching
        document is found.

        The :meth:`find_one` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        :Parameters:

          - `filter` (optional): a dictionary specifying
            the query to be performed OR any other type to be used as
            the value for a query for ``"_id"``.

          - `*args` (optional): any additional positional arguments
            are the same as the arguments to :meth:`find`.

          - `**kwargs` (optional): any additional keyword arguments
            are the same as the arguments to :meth:`find`.

              >>> collection.find_one(max_time_ms=100)
        """
        if (filter is not None and not
                isinstance(filter, abc.Mapping)):
            filter = {"_id": filter}

        cursor = self.find(filter, *args, **kwargs)
        for result in cursor.limit(-1):
            return result
        return None

    def find(self, *args, **kwargs):
        """Query the database.

        The `filter` argument is a prototype document that all results
        must match. For example:

        >>> db.test.find({"hello": "world"})

        only matches documents that have a key "hello" with value
        "world".  Matches can have other keys *in addition* to
        "hello". The `projection` argument is used to specify a subset
        of fields that should be included in the result documents. By
        limiting results to a certain subset of fields you can cut
        down on network traffic and decoding time.

        Raises :class:`TypeError` if any of the arguments are of
        improper type. Returns an instance of
        :class:`~pymongo.cursor.Cursor` corresponding to this query.

        The :meth:`find` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        :Parameters:
          - `filter` (optional): a SON object specifying elements which
            must be present for a document to be included in the
            result set
          - `projection` (optional): a list of field names that should be
            returned in the result set or a dict specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a dict to exclude fields from
            the result (e.g. projection={'_id': False}).
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `skip` (optional): the number of documents to omit (from
            the start of the result set) when returning the results
          - `limit` (optional): the maximum number of results to
            return. A limit of 0 (the default) is equivalent to setting no
            limit.
          - `no_cursor_timeout` (optional): if False (the default), any
            returned cursor is closed by the server after 10 minutes of
            inactivity. If set to True, the returned cursor will never
            time out on the server. Care should be taken to ensure that
            cursors with no_cursor_timeout turned on are properly closed.
          - `cursor_type` (optional): the type of cursor to return. The valid
            options are defined by :class:`~pymongo.cursor.CursorType`:

            - :attr:`~pymongo.cursor.CursorType.NON_TAILABLE` - the result of
              this find call will return a standard cursor over the result set.
            - :attr:`~pymongo.cursor.CursorType.TAILABLE` - the result of this
              find call will be a tailable cursor - tailable cursors are only
              for use with capped collections. They are not closed when the
              last data is retrieved but are kept open and the cursor location
              marks the final document position. If more data is received
              iteration of the cursor will continue from the last document
              received. For details, see the `tailable cursor documentation
              <http://www.mongodb.org/display/DOCS/Tailable+Cursors>`_.
            - :attr:`~pymongo.cursor.CursorType.TAILABLE_AWAIT` - the result
              of this find call will be a tailable cursor with the await flag
              set. The server will wait for a few seconds after returning the
              full result set so that it can capture and return additional data
              added during the query.
            - :attr:`~pymongo.cursor.CursorType.EXHAUST` - the result of this
              find call will be an exhaust cursor. MongoDB will stream batched
              results to the client without waiting for the client to request
              each batch, reducing latency. See notes on compatibility below.

          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for this query. See
            :meth:`~pymongo.cursor.Cursor.sort` for details.
          - `allow_partial_results` (optional): if True, mongos will return
            partial results if some shards are down instead of returning an
            error.
          - `oplog_replay` (optional): If True, set the oplogReplay query
            flag.
          - `batch_size` (optional): Limits the number of documents returned in
            a single batch.
          - `manipulate` (optional): **DEPRECATED** - If True (the default),
            apply any outgoing SON manipulators before returning.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.
          - `return_key` (optional): If True, return only the index keys in
            each document.
          - `show_record_id` (optional): If True, adds a field ``$recordId`` in
            each document with the storage engine's internal record identifier.
          - `snapshot` (optional): **DEPRECATED** - If True, prevents the
            cursor from returning a document more than once because of an
            intervening write operation.
          - `hint` (optional): An index, in the same format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.hint` on the cursor to tell Mongo the
            proper index to use for the query.
          - `max_time_ms` (optional): Specifies a time limit for a query
            operation. If the specified time is exceeded, the operation will be
            aborted and :exc:`~pymongo.errors.ExecutionTimeout` is raised. Pass
            this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.max_time_ms` on the cursor.
          - `max_scan` (optional): **DEPRECATED** - The maximum number of
            documents to scan. Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.max_scan` on the cursor.
          - `min` (optional): A list of field, limit pairs specifying the
            inclusive lower bound for all keys of a specific index in order.
            Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.min` on the cursor. ``hint`` must
            also be passed to ensure the query utilizes the correct index.
          - `max` (optional): A list of field, limit pairs specifying the
            exclusive upper bound for all keys of a specific index in order.
            Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.max` on the cursor. ``hint`` must
            also be passed to ensure the query utilizes the correct index.
          - `comment` (optional): A string to attach to the query to help
            interpret and trace the operation in the server logs and in profile
            data. Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.comment` on the cursor.
          - `modifiers` (optional): **DEPRECATED** - A dict specifying
            additional MongoDB query modifiers. Use the keyword arguments listed
            above instead.

        .. note:: There are a number of caveats to using
          :attr:`~pymongo.cursor.CursorType.EXHAUST` as cursor_type:

          - The `limit` option can not be used with an exhaust cursor.

          - Exhaust cursors are not supported by mongos and can not be
            used with a sharded cluster.

          - A :class:`~pymongo.cursor.Cursor` instance created with the
            :attr:`~pymongo.cursor.CursorType.EXHAUST` cursor_type requires an
            exclusive :class:`~socket.socket` connection to MongoDB. If the
            :class:`~pymongo.cursor.Cursor` is discarded without being
            completely iterated the underlying :class:`~socket.socket`
            connection will be closed and discarded without being returned to
            the connection pool.

        .. versionchanged:: 3.7
           Deprecated the `snapshot` option, which is deprecated in MongoDB
           3.6 and removed in MongoDB 4.0.
           Deprecated the `max_scan` option. Support for this option is
           deprecated in MongoDB 4.0. Use `max_time_ms` instead to limit server
           side execution time.


        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.5
           Added the options `return_key`, `show_record_id`, `snapshot`,
           `hint`, `max_time_ms`, `max_scan`, `min`, `max`, and `comment`.
           Deprecated the option `modifiers`.

        .. versionchanged:: 3.4
           Support the `collation` option.

        .. versionchanged:: 3.0
           Changed the parameter names `spec`, `fields`, `timeout`, and
           `partial` to `filter`, `projection`, `no_cursor_timeout`, and
           `allow_partial_results` respectively.
           Added the `cursor_type`, `oplog_replay`, and `modifiers` options.
           Removed the `network_timeout`, `read_preference`, `tag_sets`,
           `secondary_acceptable_latency_ms`, `max_scan`, `snapshot`,
           `tailable`, `await_data`, `exhaust`, `as_class`, and slave_okay
           parameters. Removed `compile_re` option: PyMongo now always
           represents BSON regular expressions as :class:`~bson.regex.Regex`
           objects. Use :meth:`~bson.regex.Regex.try_compile` to attempt to
           convert from a BSON regular expression to a Python regular
           expression object. Soft deprecated the `manipulate` option.

        .. versionchanged:: 2.7
           Added `compile_re` option. If set to False, PyMongo represented BSON
           regular expressions as :class:`~bson.regex.Regex` objects instead of
           attempting to compile BSON regular expressions as Python native
           regular expressions, thus preventing errors for some incompatible
           patterns, see `PYTHON-500`_.

        .. versionadded:: 2.3
           The `tag_sets` and `secondary_acceptable_latency_ms` parameters.

        .. _PYTHON-500: https://jira.mongodb.org/browse/PYTHON-500

        .. mongodoc:: find

        """
        return Cursor(self, *args, **kwargs)

    def find_raw_batches(self, *args, **kwargs):
        """Query the database and retrieve batches of raw BSON.

        Similar to the :meth:`find` method but returns a
        :class:`~pymongo.cursor.RawBatchCursor`.

        This example demonstrates how to work with raw batches, but in practice
        raw batches should be passed to an external library that can decode
        BSON into another data type, rather than used with PyMongo's
        :mod:`bson` module.

          >>> import bson
          >>> cursor = db.test.find_raw_batches()
          >>> for batch in cursor:
          ...     print(bson.decode_all(batch))

        .. note:: find_raw_batches does not support sessions or auto
           encryption.

        .. versionadded:: 3.6
        """
        # OP_MSG with document stream returns is required to support
        # sessions.
        if "session" in kwargs:
            raise ConfigurationError(
                "find_raw_batches does not support sessions")

        # OP_MSG is required to support encryption.
        if self.__database.client._encrypter:
            raise InvalidOperation(
                "find_raw_batches does not support auto encryption")

        return RawBatchCursor(self, *args, **kwargs)

    def parallel_scan(self, num_cursors, session=None, **kwargs):
        """**DEPRECATED**: Scan this entire collection in parallel.

        Returns a list of up to ``num_cursors`` cursors that can be iterated
        concurrently. As long as the collection is not modified during
        scanning, each document appears once in one of the cursors result
        sets.

        For example, to process each document in a collection using some
        thread-safe ``process_document()`` function:

          >>> def process_cursor(cursor):
          ...     for document in cursor:
          ...     # Some thread-safe processing function:
          ...     process_document(document)
          >>>
          >>> # Get up to 4 cursors.
          ...
          >>> cursors = collection.parallel_scan(4)
          >>> threads = [
          ...     threading.Thread(target=process_cursor, args=(cursor,))
          ...     for cursor in cursors]
          >>>
          >>> for thread in threads:
          ...     thread.start()
          >>>
          >>> for thread in threads:
          ...     thread.join()
          >>>
          >>> # All documents have now been processed.

        The :meth:`parallel_scan` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        :Parameters:
          - `num_cursors`: the number of cursors to return
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs`: additional options for the parallelCollectionScan
            command can be passed as keyword arguments.

        .. note:: Requires server version **>= 2.5.5**.

        .. versionchanged:: 3.7
           Deprecated.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Added back support for arbitrary keyword arguments. MongoDB 3.4
           adds support for maxTimeMS as an option to the
           parallelCollectionScan command.

        .. versionchanged:: 3.0
           Removed support for arbitrary keyword arguments, since
           the parallelCollectionScan command has no optional arguments.
        """
        warnings.warn("parallel_scan is deprecated. MongoDB 4.2 will remove "
                      "the parallelCollectionScan command.",
                      DeprecationWarning, stacklevel=2)
        cmd = SON([('parallelCollectionScan', self.__name),
                   ('numCursors', num_cursors)])
        cmd.update(kwargs)

        with self._socket_for_reads(session) as (sock_info, slave_ok):
            # We call sock_info.command here directly, instead of
            # calling self._command to avoid using an implicit session.
            result = sock_info.command(
                self.__database.name,
                cmd,
                slave_ok,
                self._read_preference_for(session),
                self.codec_options,
                read_concern=self.read_concern,
                parse_write_concern_error=True,
                session=session,
                client=self.__database.client)

        cursors = []
        for cursor in result['cursors']:
            cursors.append(CommandCursor(
                self, cursor['cursor'], sock_info.address,
                session=session, explicit_session=session is not None))

        return cursors

    def _count(self, cmd, collation=None, session=None):
        """Internal count helper."""
        def _cmd(session, server, sock_info, slave_ok):
            res = self._command(
                sock_info,
                cmd,
                slave_ok,
                allowable_errors=["ns missing"],
                codec_options=self.__write_response_codec_options,
                read_concern=self.read_concern,
                collation=collation,
                session=session)
            if res.get("errmsg", "") == "ns missing":
                return 0
            return int(res["n"])

        return self.__database.client._retryable_read(
            _cmd, self._read_preference_for(session), session)

    def _aggregate_one_result(
            self, sock_info, slave_ok, cmd, collation=None, session=None):
        """Internal helper to run an aggregate that returns a single result."""
        result = self._command(
            sock_info,
            cmd,
            slave_ok,
            codec_options=self.__write_response_codec_options,
            read_concern=self.read_concern,
            collation=collation,
            session=session)
        batch = result['cursor']['firstBatch']
        return batch[0] if batch else None

    def estimated_document_count(self, **kwargs):
        """Get an estimate of the number of documents in this collection using
        collection metadata.

        The :meth:`estimated_document_count` method is **not** supported in a
        transaction.

        All optional parameters should be passed as keyword arguments
        to this method. Valid options include:

          - `maxTimeMS` (int): The maximum amount of time to allow this
            operation to run, in milliseconds.

        :Parameters:
          - `**kwargs` (optional): See list of options above.

        .. versionadded:: 3.7
        """
        if 'session' in kwargs:
            raise ConfigurationError(
                'estimated_document_count does not support sessions')
        cmd = SON([('count', self.__name)])
        cmd.update(kwargs)
        return self._count(cmd)

    def count_documents(self, filter, session=None, **kwargs):
        """Count the number of documents in this collection.

        .. note:: For a fast count of the total documents in a collection see
           :meth:`estimated_document_count`.

        The :meth:`count_documents` method is supported in a transaction.

        All optional parameters should be passed as keyword arguments
        to this method. Valid options include:

          - `skip` (int): The number of matching documents to skip before
            returning results.
          - `limit` (int): The maximum number of documents to count. Must be
            a positive integer. If not provided, no limit is imposed.
          - `maxTimeMS` (int): The maximum amount of time to allow this
            operation to run, in milliseconds.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.
          - `hint` (string or list of tuples): The index to use. Specify either
            the index name as a string or the index specification as a list of
            tuples (e.g. [('a', pymongo.ASCENDING), ('b', pymongo.ASCENDING)]).
            This option is only supported on MongoDB 3.6 and above.

        The :meth:`count_documents` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        .. note:: When migrating from :meth:`count` to :meth:`count_documents`
           the following query operators must be replaced:

           +-------------+-------------------------------------+
           | Operator    | Replacement                         |
           +=============+=====================================+
           | $where      | `$expr`_                            |
           +-------------+-------------------------------------+
           | $near       | `$geoWithin`_ with `$center`_       |
           +-------------+-------------------------------------+
           | $nearSphere | `$geoWithin`_ with `$centerSphere`_ |
           +-------------+-------------------------------------+

           $expr requires MongoDB 3.6+

        :Parameters:
          - `filter` (required): A query document that selects which documents
            to count in the collection. Can be an empty document to count all
            documents.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): See list of options above.

        .. versionadded:: 3.7

        .. _$expr: https://docs.mongodb.com/manual/reference/operator/query/expr/
        .. _$geoWithin: https://docs.mongodb.com/manual/reference/operator/query/geoWithin/
        .. _$center: https://docs.mongodb.com/manual/reference/operator/query/center/#op._S_center
        .. _$centerSphere: https://docs.mongodb.com/manual/reference/operator/query/centerSphere/#op._S_centerSphere
        """
        pipeline = [{'$match': filter}]
        if 'skip' in kwargs:
            pipeline.append({'$skip': kwargs.pop('skip')})
        if 'limit' in kwargs:
            pipeline.append({'$limit': kwargs.pop('limit')})
        pipeline.append({'$group': {'_id': 1, 'n': {'$sum': 1}}})
        cmd = SON([('aggregate', self.__name),
                   ('pipeline', pipeline),
                   ('cursor', {})])
        if "hint" in kwargs and not isinstance(kwargs["hint"], string_type):
            kwargs["hint"] = helpers._index_document(kwargs["hint"])
        collation = validate_collation_or_none(kwargs.pop('collation', None))
        cmd.update(kwargs)

        def _cmd(session, server, sock_info, slave_ok):
            result = self._aggregate_one_result(
                sock_info, slave_ok, cmd, collation, session)
            if not result:
                return 0
            return result['n']

        return self.__database.client._retryable_read(
            _cmd, self._read_preference_for(session), session)

    def count(self, filter=None, session=None, **kwargs):
        """**DEPRECATED** - Get the number of documents in this collection.

        The :meth:`count` method is deprecated and **not** supported in a
        transaction. Please use :meth:`count_documents` or
        :meth:`estimated_document_count` instead.

        All optional count parameters should be passed as keyword arguments
        to this method. Valid options include:

          - `skip` (int): The number of matching documents to skip before
            returning results.
          - `limit` (int): The maximum number of documents to count. A limit
            of 0 (the default) is equivalent to setting no limit.
          - `maxTimeMS` (int): The maximum amount of time to allow the count
            command to run, in milliseconds.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.
          - `hint` (string or list of tuples): The index to use. Specify either
            the index name as a string or the index specification as a list of
            tuples (e.g. [('a', pymongo.ASCENDING), ('b', pymongo.ASCENDING)]).

        The :meth:`count` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        .. note:: When migrating from :meth:`count` to :meth:`count_documents`
           the following query operators must be replaced:

           +-------------+-------------------------------------+
           | Operator    | Replacement                         |
           +=============+=====================================+
           | $where      | `$expr`_                            |
           +-------------+-------------------------------------+
           | $near       | `$geoWithin`_ with `$center`_       |
           +-------------+-------------------------------------+
           | $nearSphere | `$geoWithin`_ with `$centerSphere`_ |
           +-------------+-------------------------------------+

           $expr requires MongoDB 3.6+

        :Parameters:
          - `filter` (optional): A query document that selects which documents
            to count in the collection.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): See list of options above.

        .. versionchanged:: 3.7
           Deprecated.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Support the `collation` option.

        .. _$expr: https://docs.mongodb.com/manual/reference/operator/query/expr/
        .. _$geoWithin: https://docs.mongodb.com/manual/reference/operator/query/geoWithin/
        .. _$center: https://docs.mongodb.com/manual/reference/operator/query/center/#op._S_center
        .. _$centerSphere: https://docs.mongodb.com/manual/reference/operator/query/centerSphere/#op._S_centerSphere
        """
        warnings.warn("count is deprecated. Use estimated_document_count or "
                      "count_documents instead. Please note that $where must "
                      "be replaced by $expr, $near must be replaced by "
                      "$geoWithin with $center, and $nearSphere must be "
                      "replaced by $geoWithin with $centerSphere",
                      DeprecationWarning, stacklevel=2)
        cmd = SON([("count", self.__name)])
        if filter is not None:
            if "query" in kwargs:
                raise ConfigurationError("can't pass both filter and query")
            kwargs["query"] = filter
        if "hint" in kwargs and not isinstance(kwargs["hint"], string_type):
            kwargs["hint"] = helpers._index_document(kwargs["hint"])
        collation = validate_collation_or_none(kwargs.pop('collation', None))
        cmd.update(kwargs)
        return self._count(cmd, collation, session)

    def create_indexes(self, indexes, session=None, **kwargs):
        """Create one or more indexes on this collection.

          >>> from pymongo import IndexModel, ASCENDING, DESCENDING
          >>> index1 = IndexModel([("hello", DESCENDING),
          ...                      ("world", ASCENDING)], name="hello_world")
          >>> index2 = IndexModel([("goodbye", DESCENDING)])
          >>> db.test.create_indexes([index1, index2])
          ["hello_world", "goodbye_-1"]

        :Parameters:
          - `indexes`: A list of :class:`~pymongo.operations.IndexModel`
            instances.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): optional arguments to the createIndexes
            command (like maxTimeMS) can be passed as keyword arguments.

        .. note:: `create_indexes` uses the `createIndexes`_ command
           introduced in MongoDB **2.6** and cannot be used with earlier
           versions.

        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation when using
           MongoDB >= 3.4.

        .. versionchanged:: 3.6
           Added ``session`` parameter. Added support for arbitrary keyword
           arguments.

        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4.
        .. versionadded:: 3.0

        .. _createIndexes: https://docs.mongodb.com/manual/reference/command/createIndexes/
        """
        common.validate_list('indexes', indexes)
        names = []
        with self._socket_for_writes(session) as sock_info:
            supports_collations = sock_info.max_wire_version >= 5
            def gen_indexes():
                for index in indexes:
                    if not isinstance(index, IndexModel):
                        raise TypeError(
                            "%r is not an instance of "
                            "pymongo.operations.IndexModel" % (index,))
                    document = index.document
                    if "collation" in document and not supports_collations:
                        raise ConfigurationError(
                            "Must be connected to MongoDB "
                            "3.4+ to use collations.")
                    names.append(document["name"])
                    yield document
            cmd = SON([('createIndexes', self.name),
                       ('indexes', list(gen_indexes()))])
            cmd.update(kwargs)
            self._command(
                sock_info, cmd, read_preference=ReadPreference.PRIMARY,
                codec_options=_UNICODE_REPLACE_CODEC_OPTIONS,
                write_concern=self._write_concern_for(session),
                session=session)
        return names

    def __create_index(self, keys, index_options, session, **kwargs):
        """Internal create index helper.

        :Parameters:
          - `keys`: a list of tuples [(key, type), (key, type), ...]
          - `index_options`: a dict of index options.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
        """
        index_doc = helpers._index_document(keys)
        index = {"key": index_doc}
        collation = validate_collation_or_none(
            index_options.pop('collation', None))
        index.update(index_options)

        with self._socket_for_writes(session) as sock_info:
            if collation is not None:
                if sock_info.max_wire_version < 5:
                    raise ConfigurationError(
                        'Must be connected to MongoDB 3.4+ to use collations.')
                else:
                    index['collation'] = collation
            cmd = SON([('createIndexes', self.name), ('indexes', [index])])
            cmd.update(kwargs)
            self._command(
                sock_info, cmd, read_preference=ReadPreference.PRIMARY,
                codec_options=_UNICODE_REPLACE_CODEC_OPTIONS,
                write_concern=self._write_concern_for(session),
                session=session)

    def create_index(self, keys, session=None, **kwargs):
        """Creates an index on this collection.

        Takes either a single key or a list of (key, direction) pairs.
        The key(s) must be an instance of :class:`basestring`
        (:class:`str` in python 3), and the direction(s) must be one of
        (:data:`~pymongo.ASCENDING`, :data:`~pymongo.DESCENDING`,
        :data:`~pymongo.GEO2D`, :data:`~pymongo.GEOHAYSTACK`,
        :data:`~pymongo.GEOSPHERE`, :data:`~pymongo.HASHED`,
        :data:`~pymongo.TEXT`).

        To create a single key ascending index on the key ``'mike'`` we just
        use a string argument::

          >>> my_collection.create_index("mike")

        For a compound index on ``'mike'`` descending and ``'eliot'``
        ascending we need to use a list of tuples::

          >>> my_collection.create_index([("mike", pymongo.DESCENDING),
          ...                             ("eliot", pymongo.ASCENDING)])

        All optional index creation parameters should be passed as
        keyword arguments to this method. For example::

          >>> my_collection.create_index([("mike", pymongo.DESCENDING)],
          ...                            background=True)

        Valid options include, but are not limited to:

          - `name`: custom name to use for this index - if none is
            given, a name will be generated.
          - `unique`: if ``True`` creates a uniqueness constraint on the index.
          - `background`: if ``True`` this index should be created in the
            background.
          - `sparse`: if ``True``, omit from the index any documents that lack
            the indexed field.
          - `bucketSize`: for use with geoHaystack indexes.
            Number of documents to group together within a certain proximity
            to a given longitude and latitude.
          - `min`: minimum value for keys in a :data:`~pymongo.GEO2D`
            index.
          - `max`: maximum value for keys in a :data:`~pymongo.GEO2D`
            index.
          - `expireAfterSeconds`: <int> Used to create an expiring (TTL)
            collection. MongoDB will automatically delete documents from
            this collection after <int> seconds. The indexed field must
            be a UTC datetime or the data will not expire.
          - `partialFilterExpression`: A document that specifies a filter for
            a partial index. Requires server version >=3.2.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.
          - `wildcardProjection`: Allows users to include or exclude specific
            field paths from a `wildcard index`_ using the { "$**" : 1} key
            pattern. Requires server version >= 4.2.

        See the MongoDB documentation for a full list of supported options by
        server version.

        .. warning:: `dropDups` is not supported by MongoDB 3.0 or newer. The
          option is silently ignored by the server and unique index builds
          using the option will fail if a duplicate value is detected.

        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation when using
           MongoDB >= 3.4.

        :Parameters:
          - `keys`: a single key or a list of (key, direction)
            pairs specifying the index to create
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): any additional index creation
            options (see the above list) should be passed as keyword
            arguments

        .. versionchanged:: 3.6
           Added ``session`` parameter. Added support for passing maxTimeMS
           in kwargs.
        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4. Support the `collation` option.
        .. versionchanged:: 3.2
            Added partialFilterExpression to support partial indexes.
        .. versionchanged:: 3.0
            Renamed `key_or_list` to `keys`. Removed the `cache_for` option.
            :meth:`create_index` no longer caches index names. Removed support
            for the drop_dups and bucket_size aliases.

        .. mongodoc:: indexes

        .. _wildcard index: https://docs.mongodb.com/master/core/index-wildcard/#wildcard-index-core
        """
        keys = helpers._index_list(keys)
        name = kwargs.setdefault("name", helpers._gen_index_name(keys))
        cmd_options = {}
        if "maxTimeMS" in kwargs:
            cmd_options["maxTimeMS"] = kwargs.pop("maxTimeMS")
        self.__create_index(keys, kwargs, session, **cmd_options)
        return name

    def ensure_index(self, key_or_list, cache_for=300, **kwargs):
        """**DEPRECATED** - Ensures that an index exists on this collection.

        .. versionchanged:: 3.0
            **DEPRECATED**
        """
        warnings.warn("ensure_index is deprecated. Use create_index instead.",
                      DeprecationWarning, stacklevel=2)
        # The types supported by datetime.timedelta.
        if not (isinstance(cache_for, integer_types) or
                isinstance(cache_for, float)):
            raise TypeError("cache_for must be an integer or float.")

        if "drop_dups" in kwargs:
            kwargs["dropDups"] = kwargs.pop("drop_dups")

        if "bucket_size" in kwargs:
            kwargs["bucketSize"] = kwargs.pop("bucket_size")

        keys = helpers._index_list(key_or_list)
        name = kwargs.setdefault("name", helpers._gen_index_name(keys))

        # Note that there is a race condition here. One thread could
        # check if the index is cached and be preempted before creating
        # and caching the index. This means multiple threads attempting
        # to create the same index concurrently could send the index
        # to the server two or more times. This has no practical impact
        # other than wasted round trips.
        if not self.__database.client._cached(self.__database.name,
                                              self.__name, name):
            self.__create_index(keys, kwargs, session=None)
            self.__database.client._cache_index(self.__database.name,
                                                self.__name, name, cache_for)
            return name
        return None

    def drop_indexes(self, session=None, **kwargs):
        """Drops all indexes on this collection.

        Can be used on non-existant collections or collections with no indexes.
        Raises OperationFailure on an error.

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): optional arguments to the createIndexes
            command (like maxTimeMS) can be passed as keyword arguments.

        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation when using
           MongoDB >= 3.4.

        .. versionchanged:: 3.6
           Added ``session`` parameter. Added support for arbitrary keyword
           arguments.

        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4.

        """
        self.__database.client._purge_index(self.__database.name, self.__name)
        self.drop_index("*", session=session, **kwargs)

    def drop_index(self, index_or_name, session=None, **kwargs):
        """Drops the specified index on this collection.

        Can be used on non-existant collections or collections with no
        indexes.  Raises OperationFailure on an error (e.g. trying to
        drop an index that does not exist). `index_or_name`
        can be either an index name (as returned by `create_index`),
        or an index specifier (as passed to `create_index`). An index
        specifier should be a list of (key, direction) pairs. Raises
        TypeError if index is not an instance of (str, unicode, list).

        .. warning::

          if a custom name was used on index creation (by
          passing the `name` parameter to :meth:`create_index` or
          :meth:`ensure_index`) the index **must** be dropped by name.

        :Parameters:
          - `index_or_name`: index (or name of index) to drop
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): optional arguments to the createIndexes
            command (like maxTimeMS) can be passed as keyword arguments.

        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation when using
           MongoDB >= 3.4.

        .. versionchanged:: 3.6
           Added ``session`` parameter. Added support for arbitrary keyword
           arguments.

        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4.

        """
        name = index_or_name
        if isinstance(index_or_name, list):
            name = helpers._gen_index_name(index_or_name)

        if not isinstance(name, string_type):
            raise TypeError("index_or_name must be an index name or list")

        self.__database.client._purge_index(
            self.__database.name, self.__name, name)
        cmd = SON([("dropIndexes", self.__name), ("index", name)])
        cmd.update(kwargs)
        with self._socket_for_writes(session) as sock_info:
            self._command(sock_info,
                          cmd,
                          read_preference=ReadPreference.PRIMARY,
                          allowable_errors=["ns not found"],
                          write_concern=self._write_concern_for(session),
                          session=session)

    def reindex(self, session=None, **kwargs):
        """Rebuilds all indexes on this collection.

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): optional arguments to the reIndex
            command (like maxTimeMS) can be passed as keyword arguments.

        .. warning:: reindex blocks all other operations (indexes
           are built in the foreground) and will be slow for large
           collections.

        .. versionchanged:: 3.6
           Added ``session`` parameter. Added support for arbitrary keyword
           arguments.

        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4.

        .. versionchanged:: 3.5
           We no longer apply this collection's write concern to this operation.
           MongoDB 3.4 silently ignored the write concern. MongoDB 3.6+ returns
           an error if we include the write concern.
        """
        cmd = SON([("reIndex", self.__name)])
        cmd.update(kwargs)
        with self._socket_for_writes(session) as sock_info:
            return self._command(
                sock_info, cmd, read_preference=ReadPreference.PRIMARY,
                session=session)

    def list_indexes(self, session=None):
        """Get a cursor over the index documents for this collection.

          >>> for index in db.test.list_indexes():
          ...     print(index)
          ...
          SON([(u'v', 1), (u'key', SON([(u'_id', 1)])),
               (u'name', u'_id_'), (u'ns', u'test.test')])

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        :Returns:
          An instance of :class:`~pymongo.command_cursor.CommandCursor`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionadded:: 3.0
        """
        codec_options = CodecOptions(SON)
        coll = self.with_options(codec_options=codec_options,
                                 read_preference=ReadPreference.PRIMARY)
        read_pref = ((session and session._txn_read_preference())
                     or ReadPreference.PRIMARY)

        def _cmd(session, server, sock_info, slave_ok):
            cmd = SON([("listIndexes", self.__name), ("cursor", {})])
            if sock_info.max_wire_version > 2:
                with self.__database.client._tmp_session(session, False) as s:
                    try:
                        cursor = self._command(sock_info, cmd, slave_ok,
                                               read_pref,
                                               codec_options,
                                               session=s)["cursor"]
                    except OperationFailure as exc:
                        # Ignore NamespaceNotFound errors to match the behavior
                        # of reading from *.system.indexes.
                        if exc.code != 26:
                            raise
                        cursor = {'id': 0, 'firstBatch': []}
                return CommandCursor(coll, cursor, sock_info.address,
                                     session=s,
                                     explicit_session=session is not None)
            else:
                res = message._first_batch(
                    sock_info, self.__database.name, "system.indexes",
                    {"ns": self.__full_name}, 0, slave_ok, codec_options,
                    read_pref, cmd,
                    self.database.client._event_listeners)
                cursor = res["cursor"]
                # Note that a collection can only have 64 indexes, so there
                # will never be a getMore call.
                return CommandCursor(coll, cursor, sock_info.address)

        return self.__database.client._retryable_read(
            _cmd, read_pref, session)

    def index_information(self, session=None):
        """Get information on this collection's indexes.

        Returns a dictionary where the keys are index names (as
        returned by create_index()) and the values are dictionaries
        containing information about each index. The dictionary is
        guaranteed to contain at least a single key, ``"key"`` which
        is a list of (key, direction) pairs specifying the index (as
        passed to create_index()). It will also contain any other
        metadata about the indexes, except for the ``"ns"`` and
        ``"name"`` keys, which are cleaned. Example output might look
        like this:

        >>> db.test.create_index("x", unique=True)
        u'x_1'
        >>> db.test.index_information()
        {u'_id_': {u'key': [(u'_id', 1)]},
         u'x_1': {u'unique': True, u'key': [(u'x', 1)]}}

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        cursor = self.list_indexes(session=session)
        info = {}
        for index in cursor:
            index["key"] = index["key"].items()
            index = dict(index)
            info[index.pop("name")] = index
        return info

    def options(self, session=None):
        """Get the options set on this collection.

        Returns a dictionary of options and their values - see
        :meth:`~pymongo.database.Database.create_collection` for more
        information on the possible options. Returns an empty
        dictionary if the collection has not been created yet.

        :Parameters:
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        dbo = self.__database.client.get_database(
            self.__database.name,
            self.codec_options,
            self.read_preference,
            self.write_concern,
            self.read_concern)
        cursor = dbo.list_collections(
            session=session, filter={"name": self.__name})

        result = None
        for doc in cursor:
            result = doc
            break

        if not result:
            return {}

        options = result.get("options", {})
        if "create" in options:
            del options["create"]

        return options

    def _aggregate(self, aggregation_command, pipeline, cursor_class, session,
                   explicit_session, **kwargs):
        # Remove things that are not command options.
        use_cursor = True
        if "useCursor" in kwargs:
            warnings.warn(
                "The useCursor option is deprecated "
                "and will be removed in PyMongo 4.0",
                DeprecationWarning, stacklevel=2)
            use_cursor = common.validate_boolean(
                "useCursor", kwargs.pop("useCursor", True))

        cmd = aggregation_command(
            self, cursor_class, pipeline, kwargs, explicit_session,
            user_fields={'cursor': {'firstBatch': 1}}, use_cursor=use_cursor)
        return self.__database.client._retryable_read(
            cmd.get_cursor, cmd.get_read_preference(session), session,
            retryable=not cmd._performs_write)

    def aggregate(self, pipeline, session=None, **kwargs):
        """Perform an aggregation using the aggregation framework on this
        collection.

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
            returning aggregate results using a cursor, or `useCursor` is
            ``False``.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.
          - `useCursor` (bool): Deprecated. Will be removed in PyMongo 4.0.

        The :meth:`aggregate` method obeys the :attr:`read_preference` of this
        :class:`Collection`, except when ``$out`` or ``$merge`` are used, in
        which case  :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY`
        is used.

        .. note:: This method does not support the 'explain' option. Please
           use :meth:`~pymongo.database.Database.command` instead. An
           example is included in the :ref:`aggregate-examples` documentation.

        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation when using
           MongoDB >= 3.4.

        :Parameters:
          - `pipeline`: a list of aggregation pipeline stages
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): See list of options above.

        :Returns:
          A :class:`~pymongo.command_cursor.CommandCursor` over the result
          set.

        .. versionchanged:: 3.9
           Apply this collection's read concern to pipelines containing the
           `$out` stage when connected to MongoDB >= 4.2.
           Added support for the ``$merge`` pipeline stage.
           Aggregations that write always use read preference
           :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY`.
        .. versionchanged:: 3.6
           Added the `session` parameter. Added the `maxAwaitTimeMS` option.
           Deprecated the `useCursor` option.
        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4. Support the `collation` option.
        .. versionchanged:: 3.0
           The :meth:`aggregate` method always returns a CommandCursor. The
           pipeline argument must be a list.
        .. versionchanged:: 2.7
           When the cursor option is used, return
           :class:`~pymongo.command_cursor.CommandCursor` instead of
           :class:`~pymongo.cursor.Cursor`.
        .. versionchanged:: 2.6
           Added cursor support.
        .. versionadded:: 2.3

        .. seealso:: :doc:`/examples/aggregation`

        .. _aggregate command:
            https://docs.mongodb.com/manual/reference/command/aggregate
        """
        with self.__database.client._tmp_session(session, close=False) as s:
            return self._aggregate(_CollectionAggregationCommand,
                                   pipeline,
                                   CommandCursor,
                                   session=s,
                                   explicit_session=session is not None,
                                   **kwargs)

    def aggregate_raw_batches(self, pipeline, **kwargs):
        """Perform an aggregation and retrieve batches of raw BSON.

        Similar to the :meth:`aggregate` method but returns a
        :class:`~pymongo.cursor.RawBatchCursor`.

        This example demonstrates how to work with raw batches, but in practice
        raw batches should be passed to an external library that can decode
        BSON into another data type, rather than used with PyMongo's
        :mod:`bson` module.

          >>> import bson
          >>> cursor = db.test.aggregate_raw_batches([
          ...     {'$project': {'x': {'$multiply': [2, '$x']}}}])
          >>> for batch in cursor:
          ...     print(bson.decode_all(batch))

        .. note:: aggregate_raw_batches does not support sessions or auto
           encryption.

        .. versionadded:: 3.6
        """
        # OP_MSG with document stream returns is required to support
        # sessions.
        if "session" in kwargs:
            raise ConfigurationError(
                "aggregate_raw_batches does not support sessions")

        # OP_MSG is required to support encryption.
        if self.__database.client._encrypter:
            raise InvalidOperation(
                "aggregate_raw_batches does not support auto encryption")

        return self._aggregate(_CollectionRawAggregationCommand,
                               pipeline,
                               RawBatchCommandCursor,
                               session=None,
                               explicit_session=False,
                               **kwargs)

    def watch(self, pipeline=None, full_document=None, resume_after=None,
              max_await_time_ms=None, batch_size=None, collation=None,
              start_at_operation_time=None, session=None, start_after=None):
        """Watch changes on this collection.

        Performs an aggregation with an implicit initial ``$changeStream``
        stage and returns a
        :class:`~pymongo.change_stream.CollectionChangeStream` cursor which
        iterates over changes on this collection.

        Introduced in MongoDB 3.6.

        .. code-block:: python

           with db.collection.watch() as stream:
               for change in stream:
                   print(change)

        The :class:`~pymongo.change_stream.CollectionChangeStream` iterable
        blocks until the next change document is returned or an error is
        raised. If the
        :meth:`~pymongo.change_stream.CollectionChangeStream.next` method
        encounters a network error when retrieving a batch from the server,
        it will automatically attempt to recreate the cursor such that no
        change events are missed. Any error encountered during the resume
        attempt indicates there may be an outage and will be raised.

        .. code-block:: python

            try:
                with db.collection.watch(
                        [{'$match': {'operationType': 'insert'}}]) as stream:
                    for insert_change in stream:
                        print(insert_change)
            except pymongo.errors.PyMongoError:
                # The ChangeStream encountered an unrecoverable error or the
                # resume attempt failed to recreate the cursor.
                logging.error('...')

        For a precise description of the resume process see the
        `change streams specification`_.

        .. note:: Using this helper method is preferred to directly calling
            :meth:`~pymongo.collection.Collection.aggregate` with a
            ``$changeStream`` stage, for the purpose of supporting
            resumability.

        .. warning:: This Collection's :attr:`read_concern` must be
            ``ReadConcern("majority")`` in order to use the ``$changeStream``
            stage.

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
          A :class:`~pymongo.change_stream.CollectionChangeStream` cursor.

        .. versionchanged:: 3.9
           Added the ``start_after`` parameter.

        .. versionchanged:: 3.7
           Added the ``start_at_operation_time`` parameter.

        .. versionadded:: 3.6

        .. mongodoc:: changeStreams

        .. _change streams specification:
            https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst
        """
        return CollectionChangeStream(
            self, pipeline, full_document, resume_after, max_await_time_ms,
            batch_size, collation, start_at_operation_time, session,
            start_after)

    def group(self, key, condition, initial, reduce, finalize=None, **kwargs):
        """Perform a query similar to an SQL *group by* operation.

        **DEPRECATED** - The group command was deprecated in MongoDB 3.4. The
        :meth:`~group` method is deprecated and will be removed in PyMongo 4.0.
        Use :meth:`~aggregate` with the `$group` stage or :meth:`~map_reduce`
        instead.

        .. versionchanged:: 3.5
           Deprecated the group method.
        .. versionchanged:: 3.4
           Added the `collation` option.
        .. versionchanged:: 2.2
           Removed deprecated argument: command
        """
        warnings.warn("The group method is deprecated and will be removed in "
                      "PyMongo 4.0. Use the aggregate method with the $group "
                      "stage or the map_reduce method instead.",
                      DeprecationWarning, stacklevel=2)
        group = {}
        if isinstance(key, string_type):
            group["$keyf"] = Code(key)
        elif key is not None:
            group = {"key": helpers._fields_list_to_dict(key, "key")}
        group["ns"] = self.__name
        group["$reduce"] = Code(reduce)
        group["cond"] = condition
        group["initial"] = initial
        if finalize is not None:
            group["finalize"] = Code(finalize)

        cmd = SON([("group", group)])
        collation = validate_collation_or_none(kwargs.pop('collation', None))
        cmd.update(kwargs)

        with self._socket_for_reads(session=None) as (sock_info, slave_ok):
            return self._command(sock_info, cmd, slave_ok,
                                 collation=collation,
                                 user_fields={'retval': 1})["retval"]

    def rename(self, new_name, session=None, **kwargs):
        """Rename this collection.

        If operating in auth mode, client must be authorized as an
        admin to perform this operation. Raises :class:`TypeError` if
        `new_name` is not an instance of :class:`basestring`
        (:class:`str` in python 3). Raises :class:`~pymongo.errors.InvalidName`
        if `new_name` is not a valid collection name.

        :Parameters:
          - `new_name`: new name for this collection
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): additional arguments to the rename command
            may be passed as keyword arguments to this helper method
            (i.e. ``dropTarget=True``)

        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation when using
           MongoDB >= 3.4.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4.

        """
        if not isinstance(new_name, string_type):
            raise TypeError("new_name must be an "
                            "instance of %s" % (string_type.__name__,))

        if not new_name or ".." in new_name:
            raise InvalidName("collection names cannot be empty")
        if new_name[0] == "." or new_name[-1] == ".":
            raise InvalidName("collecion names must not start or end with '.'")
        if "$" in new_name and not new_name.startswith("oplog.$main"):
            raise InvalidName("collection names must not contain '$'")

        new_name = "%s.%s" % (self.__database.name, new_name)
        cmd = SON([("renameCollection", self.__full_name), ("to", new_name)])
        cmd.update(kwargs)
        write_concern = self._write_concern_for_cmd(cmd, session)

        with self._socket_for_writes(session) as sock_info:
            with self.__database.client._tmp_session(session) as s:
                return sock_info.command(
                    'admin', cmd,
                    write_concern=write_concern,
                    parse_write_concern_error=True,
                    session=s, client=self.__database.client)

    def distinct(self, key, filter=None, session=None, **kwargs):
        """Get a list of distinct values for `key` among all documents
        in this collection.

        Raises :class:`TypeError` if `key` is not an instance of
        :class:`basestring` (:class:`str` in python 3).

        All optional distinct parameters should be passed as keyword arguments
        to this method. Valid options include:

          - `maxTimeMS` (int): The maximum amount of time to allow the count
            command to run, in milliseconds.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only supported
            on MongoDB 3.4 and above.

        The :meth:`distinct` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        :Parameters:
          - `key`: name of the field for which we want to get the distinct
            values
          - `filter` (optional): A query document that specifies the documents
            from which to retrieve the distinct values.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): See list of options above.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Support the `collation` option.

        """
        if not isinstance(key, string_type):
            raise TypeError("key must be an "
                            "instance of %s" % (string_type.__name__,))
        cmd = SON([("distinct", self.__name),
                   ("key", key)])
        if filter is not None:
            if "query" in kwargs:
                raise ConfigurationError("can't pass both filter and query")
            kwargs["query"] = filter
        collation = validate_collation_or_none(kwargs.pop('collation', None))
        cmd.update(kwargs)
        def _cmd(session, server, sock_info, slave_ok):
            return self._command(
                sock_info, cmd, slave_ok, read_concern=self.read_concern,
                collation=collation, session=session,
                user_fields={"values": 1})["values"]

        return self.__database.client._retryable_read(
            _cmd, self._read_preference_for(session), session)

    def _map_reduce(self, map, reduce, out, session, read_pref, **kwargs):
        """Internal mapReduce helper."""
        cmd = SON([("mapReduce", self.__name),
                   ("map", map),
                   ("reduce", reduce),
                   ("out", out)])
        collation = validate_collation_or_none(kwargs.pop('collation', None))
        cmd.update(kwargs)

        inline = 'inline' in out

        if inline:
            user_fields = {'results': 1}
        else:
            user_fields = None

        read_pref = ((session and session._txn_read_preference())
                     or read_pref)

        with self.__database.client._socket_for_reads(read_pref, session) as (
                sock_info, slave_ok):
            if (sock_info.max_wire_version >= 4 and
                    ('readConcern' not in cmd) and
                    inline):
                read_concern = self.read_concern
            else:
                read_concern = None
            if 'writeConcern' not in cmd and not inline:
                write_concern = self._write_concern_for(session)
            else:
                write_concern = None

            return self._command(
                sock_info, cmd, slave_ok, read_pref,
                read_concern=read_concern,
                write_concern=write_concern,
                collation=collation, session=session,
                user_fields=user_fields)

    def map_reduce(self, map, reduce, out, full_response=False, session=None,
                   **kwargs):
        """Perform a map/reduce operation on this collection.

        If `full_response` is ``False`` (default) returns a
        :class:`~pymongo.collection.Collection` instance containing
        the results of the operation. Otherwise, returns the full
        response from the server to the `map reduce command`_.

        :Parameters:
          - `map`: map function (as a JavaScript string)
          - `reduce`: reduce function (as a JavaScript string)
          - `out`: output collection name or `out object` (dict). See
            the `map reduce command`_ documentation for available options.
            Note: `out` options are order sensitive. :class:`~bson.son.SON`
            can be used to specify multiple options.
            e.g. SON([('replace', <collection name>), ('db', <database name>)])
          - `full_response` (optional): if ``True``, return full response to
            this command - otherwise just return the result collection
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): additional arguments to the
            `map reduce command`_ may be passed as keyword arguments to this
            helper method, e.g.::

            >>> db.test.map_reduce(map, reduce, "myresults", limit=2)

        .. note:: The :meth:`map_reduce` method does **not** obey the
           :attr:`read_preference` of this :class:`Collection`. To run
           mapReduce on a secondary use the :meth:`inline_map_reduce` method
           instead.

        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation (if the
           output is not inline) when using MongoDB >= 3.4.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4.

        .. seealso:: :doc:`/examples/aggregation`

        .. versionchanged:: 3.4
           Added the `collation` option.
        .. versionchanged:: 2.2
           Removed deprecated arguments: merge_output and reduce_output

        .. _map reduce command: http://docs.mongodb.org/manual/reference/command/mapReduce/

        .. mongodoc:: mapreduce

        """
        if not isinstance(out, (string_type, abc.Mapping)):
            raise TypeError("'out' must be an instance of "
                            "%s or a mapping" % (string_type.__name__,))

        response = self._map_reduce(map, reduce, out, session,
                                    ReadPreference.PRIMARY, **kwargs)

        if full_response or not response.get('result'):
            return response
        elif isinstance(response['result'], dict):
            dbase = response['result']['db']
            coll = response['result']['collection']
            return self.__database.client[dbase][coll]
        else:
            return self.__database[response["result"]]

    def inline_map_reduce(self, map, reduce, full_response=False, session=None,
                          **kwargs):
        """Perform an inline map/reduce operation on this collection.

        Perform the map/reduce operation on the server in RAM. A result
        collection is not created. The result set is returned as a list
        of documents.

        If `full_response` is ``False`` (default) returns the
        result documents in a list. Otherwise, returns the full
        response from the server to the `map reduce command`_.

        The :meth:`inline_map_reduce` method obeys the :attr:`read_preference`
        of this :class:`Collection`.

        :Parameters:
          - `map`: map function (as a JavaScript string)
          - `reduce`: reduce function (as a JavaScript string)
          - `full_response` (optional): if ``True``, return full response to
            this command - otherwise just return the result collection
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): additional arguments to the
            `map reduce command`_ may be passed as keyword arguments to this
            helper method, e.g.::

            >>> db.test.inline_map_reduce(map, reduce, limit=2)

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Added the `collation` option.

        """
        res = self._map_reduce(map, reduce, {"inline": 1}, session,
                               self.read_preference, **kwargs)

        if full_response:
            return res
        else:
            return res.get("results")

    def _write_concern_for_cmd(self, cmd, session):
        raw_wc = cmd.get('writeConcern')
        if raw_wc is not None:
            return WriteConcern(**raw_wc)
        else:
            return self._write_concern_for(session)

    def __find_and_modify(self, filter, projection, sort, upsert=None,
                          return_document=ReturnDocument.BEFORE,
                          array_filters=None, session=None, **kwargs):
        """Internal findAndModify helper."""

        common.validate_is_mapping("filter", filter)
        if not isinstance(return_document, bool):
            raise ValueError("return_document must be "
                             "ReturnDocument.BEFORE or ReturnDocument.AFTER")
        collation = validate_collation_or_none(kwargs.pop('collation', None))
        cmd = SON([("findAndModify", self.__name),
                   ("query", filter),
                   ("new", return_document)])
        cmd.update(kwargs)
        if projection is not None:
            cmd["fields"] = helpers._fields_list_to_dict(projection,
                                                         "projection")
        if sort is not None:
            cmd["sort"] = helpers._index_document(sort)
        if upsert is not None:
            common.validate_boolean("upsert", upsert)
            cmd["upsert"] = upsert

        write_concern = self._write_concern_for_cmd(cmd, session)

        def _find_and_modify(session, sock_info, retryable_write):
            if array_filters is not None:
                if sock_info.max_wire_version < 6:
                    raise ConfigurationError(
                        'Must be connected to MongoDB 3.6+ to use '
                        'arrayFilters.')
                if not write_concern.acknowledged:
                    raise ConfigurationError(
                        'arrayFilters is unsupported for unacknowledged '
                        'writes.')
                cmd["arrayFilters"] = array_filters
            if (sock_info.max_wire_version >= 4 and
                    not write_concern.is_server_default):
                cmd['writeConcern'] = write_concern.document
            out = self._command(sock_info, cmd,
                                read_preference=ReadPreference.PRIMARY,
                                write_concern=write_concern,
                                allowable_errors=[_NO_OBJ_ERROR],
                                collation=collation, session=session,
                                retryable_write=retryable_write,
                                user_fields=_FIND_AND_MODIFY_DOC_FIELDS)
            _check_write_command_response(out)

            return out.get("value")

        return self.__database.client._retryable_write(
            write_concern.acknowledged, _find_and_modify, session)

    def find_one_and_delete(self, filter,
                            projection=None, sort=None, session=None, **kwargs):
        """Finds a single document and deletes it, returning the document.

          >>> db.test.count_documents({'x': 1})
          2
          >>> db.test.find_one_and_delete({'x': 1})
          {u'x': 1, u'_id': ObjectId('54f4e12bfba5220aa4d6dee8')}
          >>> db.test.count_documents({'x': 1})
          1

        If multiple documents match *filter*, a *sort* can be applied.

          >>> for doc in db.test.find({'x': 1}):
          ...     print(doc)
          ...
          {u'x': 1, u'_id': 0}
          {u'x': 1, u'_id': 1}
          {u'x': 1, u'_id': 2}
          >>> db.test.find_one_and_delete(
          ...     {'x': 1}, sort=[('_id', pymongo.DESCENDING)])
          {u'x': 1, u'_id': 2}

        The *projection* option can be used to limit the fields returned.

          >>> db.test.find_one_and_delete({'x': 1}, projection={'_id': False})
          {u'x': 1}

        :Parameters:
          - `filter`: A query that matches the document to delete.
          - `projection` (optional): a list of field names that should be
            returned in the result document or a mapping specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a mapping to exclude fields from
            the result (e.g. projection={'_id': False}).
          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for the query. If multiple documents
            match the query, they are sorted and the first is deleted.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): additional command arguments can be passed
            as keyword arguments (for example maxTimeMS can be used with
            recent server versions).

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.2
           Respects write concern.

        .. warning:: Starting in PyMongo 3.2, this command uses the
           :class:`~pymongo.write_concern.WriteConcern` of this
           :class:`~pymongo.collection.Collection` when connected to MongoDB >=
           3.2. Note that using an elevated write concern with this command may
           be slower compared to using the default write concern.

        .. versionchanged:: 3.4
           Added the `collation` option.
        .. versionadded:: 3.0

        """
        kwargs['remove'] = True
        return self.__find_and_modify(filter, projection, sort,
                                      session=session, **kwargs)

    def find_one_and_replace(self, filter, replacement,
                             projection=None, sort=None, upsert=False,
                             return_document=ReturnDocument.BEFORE,
                             session=None, **kwargs):
        """Finds a single document and replaces it, returning either the
        original or the replaced document.

        The :meth:`find_one_and_replace` method differs from
        :meth:`find_one_and_update` by replacing the document matched by
        *filter*, rather than modifying the existing document.

          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {u'x': 1, u'_id': 0}
          {u'x': 1, u'_id': 1}
          {u'x': 1, u'_id': 2}
          >>> db.test.find_one_and_replace({'x': 1}, {'y': 1})
          {u'x': 1, u'_id': 0}
          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {u'y': 1, u'_id': 0}
          {u'x': 1, u'_id': 1}
          {u'x': 1, u'_id': 2}

        :Parameters:
          - `filter`: A query that matches the document to replace.
          - `replacement`: The replacement document.
          - `projection` (optional): A list of field names that should be
            returned in the result document or a mapping specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a mapping to exclude fields from
            the result (e.g. projection={'_id': False}).
          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for the query. If multiple documents
            match the query, they are sorted and the first is replaced.
          - `upsert` (optional): When ``True``, inserts a new document if no
            document matches the query. Defaults to ``False``.
          - `return_document`: If
            :attr:`ReturnDocument.BEFORE` (the default),
            returns the original document before it was replaced, or ``None``
            if no document matches. If
            :attr:`ReturnDocument.AFTER`, returns the replaced
            or inserted document.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): additional command arguments can be passed
            as keyword arguments (for example maxTimeMS can be used with
            recent server versions).

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        .. versionchanged:: 3.4
           Added the `collation` option.
        .. versionchanged:: 3.2
           Respects write concern.

        .. warning:: Starting in PyMongo 3.2, this command uses the
           :class:`~pymongo.write_concern.WriteConcern` of this
           :class:`~pymongo.collection.Collection` when connected to MongoDB >=
           3.2. Note that using an elevated write concern with this command may
           be slower compared to using the default write concern.

        .. versionadded:: 3.0
        """
        common.validate_ok_for_replace(replacement)
        kwargs['update'] = replacement
        return self.__find_and_modify(filter, projection,
                                      sort, upsert, return_document,
                                      session=session, **kwargs)

    def find_one_and_update(self, filter, update,
                            projection=None, sort=None, upsert=False,
                            return_document=ReturnDocument.BEFORE,
                            array_filters=None, session=None, **kwargs):
        """Finds a single document and updates it, returning either the
        original or the updated document.

          >>> db.test.find_one_and_update(
          ...    {'_id': 665}, {'$inc': {'count': 1}, '$set': {'done': True}})
          {u'_id': 665, u'done': False, u'count': 25}}

        Returns ``None`` if no document matches the filter.

          >>> db.test.find_one_and_update(
          ...    {'_exists': False}, {'$inc': {'count': 1}})

        When the filter matches, by default :meth:`find_one_and_update`
        returns the original version of the document before the update was
        applied. To return the updated (or inserted in the case of
        *upsert*) version of the document instead, use the *return_document*
        option.

          >>> from pymongo import ReturnDocument
          >>> db.example.find_one_and_update(
          ...     {'_id': 'userid'},
          ...     {'$inc': {'seq': 1}},
          ...     return_document=ReturnDocument.AFTER)
          {u'_id': u'userid', u'seq': 1}

        You can limit the fields returned with the *projection* option.

          >>> db.example.find_one_and_update(
          ...     {'_id': 'userid'},
          ...     {'$inc': {'seq': 1}},
          ...     projection={'seq': True, '_id': False},
          ...     return_document=ReturnDocument.AFTER)
          {u'seq': 2}

        The *upsert* option can be used to create the document if it doesn't
        already exist.

          >>> db.example.delete_many({}).deleted_count
          1
          >>> db.example.find_one_and_update(
          ...     {'_id': 'userid'},
          ...     {'$inc': {'seq': 1}},
          ...     projection={'seq': True, '_id': False},
          ...     upsert=True,
          ...     return_document=ReturnDocument.AFTER)
          {u'seq': 1}

        If multiple documents match *filter*, a *sort* can be applied.

          >>> for doc in db.test.find({'done': True}):
          ...     print(doc)
          ...
          {u'_id': 665, u'done': True, u'result': {u'count': 26}}
          {u'_id': 701, u'done': True, u'result': {u'count': 17}}
          >>> db.test.find_one_and_update(
          ...     {'done': True},
          ...     {'$set': {'final': True}},
          ...     sort=[('_id', pymongo.DESCENDING)])
          {u'_id': 701, u'done': True, u'result': {u'count': 17}}

        :Parameters:
          - `filter`: A query that matches the document to update.
          - `update`: The update operations to apply.
          - `projection` (optional): A list of field names that should be
            returned in the result document or a mapping specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a dict to exclude fields from
            the result (e.g. projection={'_id': False}).
          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for the query. If multiple documents
            match the query, they are sorted and the first is updated.
          - `upsert` (optional): When ``True``, inserts a new document if no
            document matches the query. Defaults to ``False``.
          - `return_document`: If
            :attr:`ReturnDocument.BEFORE` (the default),
            returns the original document before it was updated. If
            :attr:`ReturnDocument.AFTER`, returns the updated
            or inserted document.
          - `array_filters` (optional): A list of filters specifying which
            array elements an update should apply. Requires MongoDB 3.6+.
          - `session` (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          - `**kwargs` (optional): additional command arguments can be passed
            as keyword arguments (for example maxTimeMS can be used with
            recent server versions).

        .. versionchanged:: 3.9
           Added the ability to accept a pipeline as the `update`.
        .. versionchanged:: 3.6
           Added the `array_filters` and `session` options.
        .. versionchanged:: 3.4
           Added the `collation` option.
        .. versionchanged:: 3.2
           Respects write concern.

        .. warning:: Starting in PyMongo 3.2, this command uses the
           :class:`~pymongo.write_concern.WriteConcern` of this
           :class:`~pymongo.collection.Collection` when connected to MongoDB >=
           3.2. Note that using an elevated write concern with this command may
           be slower compared to using the default write concern.

        .. versionadded:: 3.0
        """
        common.validate_ok_for_update(update)
        common.validate_list_or_none('array_filters', array_filters)
        kwargs['update'] = update
        return self.__find_and_modify(filter, projection,
                                      sort, upsert, return_document,
                                      array_filters, session=session, **kwargs)

    def save(self, to_save, manipulate=True, check_keys=True, **kwargs):
        """Save a document in this collection.

        **DEPRECATED** - Use :meth:`insert_one` or :meth:`replace_one` instead.

        .. versionchanged:: 3.0
           Removed the `safe` parameter. Pass ``w=0`` for unacknowledged write
           operations.
        """
        warnings.warn("save is deprecated. Use insert_one or replace_one "
                      "instead", DeprecationWarning, stacklevel=2)
        common.validate_is_document_type("to_save", to_save)

        write_concern = None
        collation = validate_collation_or_none(kwargs.pop('collation', None))
        if kwargs:
            write_concern = WriteConcern(**kwargs)

        if not (isinstance(to_save, RawBSONDocument) or "_id" in to_save):
            return self._insert(
                to_save, True, check_keys, manipulate, write_concern)
        else:
            self._update_retryable(
                {"_id": to_save["_id"]}, to_save, True,
                check_keys, False, manipulate, write_concern,
                collation=collation)
            return to_save.get("_id")

    def insert(self, doc_or_docs, manipulate=True,
               check_keys=True, continue_on_error=False, **kwargs):
        """Insert a document(s) into this collection.

        **DEPRECATED** - Use :meth:`insert_one` or :meth:`insert_many` instead.

        .. versionchanged:: 3.0
           Removed the `safe` parameter. Pass ``w=0`` for unacknowledged write
           operations.
        """
        warnings.warn("insert is deprecated. Use insert_one or insert_many "
                      "instead.", DeprecationWarning, stacklevel=2)
        write_concern = None
        if kwargs:
            write_concern = WriteConcern(**kwargs)
        return self._insert(doc_or_docs, not continue_on_error,
                            check_keys, manipulate, write_concern)

    def update(self, spec, document, upsert=False, manipulate=False,
               multi=False, check_keys=True, **kwargs):
        """Update a document(s) in this collection.

        **DEPRECATED** - Use :meth:`replace_one`, :meth:`update_one`, or
        :meth:`update_many` instead.

        .. versionchanged:: 3.0
           Removed the `safe` parameter. Pass ``w=0`` for unacknowledged write
           operations.
        """
        warnings.warn("update is deprecated. Use replace_one, update_one or "
                      "update_many instead.", DeprecationWarning, stacklevel=2)
        common.validate_is_mapping("spec", spec)
        common.validate_is_mapping("document", document)
        if document:
            # If a top level key begins with '$' this is a modify operation
            # and we should skip key validation. It doesn't matter which key
            # we check here. Passing a document with a mix of top level keys
            # starting with and without a '$' is invalid and the server will
            # raise an appropriate exception.
            first = next(iter(document))
            if first.startswith('$'):
                check_keys = False

        write_concern = None
        collation = validate_collation_or_none(kwargs.pop('collation', None))
        if kwargs:
            write_concern = WriteConcern(**kwargs)
        return self._update_retryable(
            spec, document, upsert, check_keys, multi, manipulate,
            write_concern, collation=collation)

    def remove(self, spec_or_id=None, multi=True, **kwargs):
        """Remove a document(s) from this collection.

        **DEPRECATED** - Use :meth:`delete_one` or :meth:`delete_many` instead.

        .. versionchanged:: 3.0
           Removed the `safe` parameter. Pass ``w=0`` for unacknowledged write
           operations.
        """
        warnings.warn("remove is deprecated. Use delete_one or delete_many "
                      "instead.", DeprecationWarning, stacklevel=2)
        if spec_or_id is None:
            spec_or_id = {}
        if not isinstance(spec_or_id, abc.Mapping):
            spec_or_id = {"_id": spec_or_id}
        write_concern = None
        collation = validate_collation_or_none(kwargs.pop('collation', None))
        if kwargs:
            write_concern = WriteConcern(**kwargs)
        return self._delete_retryable(
            spec_or_id, multi, write_concern, collation=collation)

    def find_and_modify(self, query={}, update=None,
                        upsert=False, sort=None, full_response=False,
                        manipulate=False, **kwargs):
        """Update and return an object.

        **DEPRECATED** - Use :meth:`find_one_and_delete`,
        :meth:`find_one_and_replace`, or :meth:`find_one_and_update` instead.
        """
        warnings.warn("find_and_modify is deprecated, use find_one_and_delete"
                      ", find_one_and_replace, or find_one_and_update instead",
                      DeprecationWarning, stacklevel=2)

        if not update and not kwargs.get('remove', None):
            raise ValueError("Must either update or remove")

        if update and kwargs.get('remove', None):
            raise ValueError("Can't do both update and remove")

        # No need to include empty args
        if query:
            kwargs['query'] = query
        if update:
            kwargs['update'] = update
        if upsert:
            kwargs['upsert'] = upsert
        if sort:
            # Accept a list of tuples to match Cursor's sort parameter.
            if isinstance(sort, list):
                kwargs['sort'] = helpers._index_document(sort)
            # Accept OrderedDict, SON, and dict with len == 1 so we
            # don't break existing code already using find_and_modify.
            elif (isinstance(sort, ORDERED_TYPES) or
                  isinstance(sort, dict) and len(sort) == 1):
                warnings.warn("Passing mapping types for `sort` is deprecated,"
                              " use a list of (key, direction) pairs instead",
                              DeprecationWarning, stacklevel=2)
                kwargs['sort'] = sort
            else:
                raise TypeError("sort must be a list of (key, direction) "
                                "pairs, a dict of len 1, or an instance of "
                                "SON or OrderedDict")

        fields = kwargs.pop("fields", None)
        if fields is not None:
            kwargs["fields"] = helpers._fields_list_to_dict(fields, "fields")

        collation = validate_collation_or_none(kwargs.pop('collation', None))

        cmd = SON([("findAndModify", self.__name)])
        cmd.update(kwargs)

        write_concern = self._write_concern_for_cmd(cmd, None)

        def _find_and_modify(session, sock_info, retryable_write):
            if (sock_info.max_wire_version >= 4 and
                    not write_concern.is_server_default):
                cmd['writeConcern'] = write_concern.document
            result = self._command(
                sock_info, cmd, read_preference=ReadPreference.PRIMARY,
                allowable_errors=[_NO_OBJ_ERROR], collation=collation,
                session=session, retryable_write=retryable_write,
                user_fields=_FIND_AND_MODIFY_DOC_FIELDS)

            _check_write_command_response(result)
            return result

        out = self.__database.client._retryable_write(
            write_concern.acknowledged, _find_and_modify, None)

        if not out['ok']:
            if out["errmsg"] == _NO_OBJ_ERROR:
                return None
            else:
                # Should never get here b/c of allowable_errors
                raise ValueError("Unexpected Error: %s" % (out,))

        if full_response:
            return out
        else:
            document = out.get('value')
            if manipulate:
                document = self.__database._fix_outgoing(document, self)
            return document

    def __iter__(self):
        return self

    def __next__(self):
        raise TypeError("'Collection' object is not iterable")

    next = __next__

    def __call__(self, *args, **kwargs):
        """This is only here so that some API misusages are easier to debug.
        """
        if "." not in self.__name:
            raise TypeError("'Collection' object is not callable. If you "
                            "meant to call the '%s' method on a 'Database' "
                            "object it is failing because no such method "
                            "exists." %
                            self.__name)
        raise TypeError("'Collection' object is not callable. If you meant to "
                        "call the '%s' method on a 'Collection' object it is "
                        "failing because no such method exists." %
                        self.__name.split(".")[-1])
