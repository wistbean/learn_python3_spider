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

"""The bulk write operations interface.

.. versionadded:: 2.7
"""
import copy

from itertools import islice

from bson.objectid import ObjectId
from bson.raw_bson import RawBSONDocument
from bson.son import SON
from pymongo.client_session import _validate_session_write_concern
from pymongo.common import (validate_is_mapping,
                            validate_is_document_type,
                            validate_ok_for_replace,
                            validate_ok_for_update)
from pymongo.helpers import _RETRYABLE_ERROR_CODES
from pymongo.collation import validate_collation_or_none
from pymongo.errors import (BulkWriteError,
                            ConfigurationError,
                            InvalidOperation,
                            OperationFailure)
from pymongo.message import (_INSERT, _UPDATE, _DELETE,
                             _do_batched_insert,
                             _randint,
                             _BulkWriteContext,
                             _EncryptedBulkWriteContext)
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern


_DELETE_ALL = 0
_DELETE_ONE = 1

# For backwards compatibility. See MongoDB src/mongo/base/error_codes.err
_BAD_VALUE = 2
_UNKNOWN_ERROR = 8
_WRITE_CONCERN_ERROR = 64

_COMMANDS = ('insert', 'update', 'delete')


# These string literals are used when we create fake server return
# documents client side. We use unicode literals in python 2.x to
# match the actual return values from the server.
_UOP = u"op"


class _Run(object):
    """Represents a batch of write operations.
    """
    def __init__(self, op_type):
        """Initialize a new Run object.
        """
        self.op_type = op_type
        self.index_map = []
        self.ops = []
        self.idx_offset = 0

    def index(self, idx):
        """Get the original index of an operation in this run.

        :Parameters:
          - `idx`: The Run index that maps to the original index.
        """
        return self.index_map[idx]

    def add(self, original_index, operation):
        """Add an operation to this Run instance.

        :Parameters:
          - `original_index`: The original index of this operation
            within a larger bulk operation.
          - `operation`: The operation document.
        """
        self.index_map.append(original_index)
        self.ops.append(operation)


def _merge_command(run, full_result, offset, result):
    """Merge a write command result into the full bulk result.
    """
    affected = result.get("n", 0)

    if run.op_type == _INSERT:
        full_result["nInserted"] += affected

    elif run.op_type == _DELETE:
        full_result["nRemoved"] += affected

    elif run.op_type == _UPDATE:
        upserted = result.get("upserted")
        if upserted:
            n_upserted = len(upserted)
            for doc in upserted:
                doc["index"] = run.index(doc["index"] + offset)
            full_result["upserted"].extend(upserted)
            full_result["nUpserted"] += n_upserted
            full_result["nMatched"] += (affected - n_upserted)
        else:
            full_result["nMatched"] += affected
        full_result["nModified"] += result["nModified"]

    write_errors = result.get("writeErrors")
    if write_errors:
        for doc in write_errors:
            # Leave the server response intact for APM.
            replacement = doc.copy()
            idx = doc["index"] + offset
            replacement["index"] = run.index(idx)
            # Add the failed operation to the error document.
            replacement[_UOP] = run.ops[idx]
            full_result["writeErrors"].append(replacement)

    wc_error = result.get("writeConcernError")
    if wc_error:
        full_result["writeConcernErrors"].append(wc_error)


def _raise_bulk_write_error(full_result):
    """Raise a BulkWriteError from the full bulk api result.
    """
    if full_result["writeErrors"]:
        full_result["writeErrors"].sort(
            key=lambda error: error["index"])
    raise BulkWriteError(full_result)


class _Bulk(object):
    """The private guts of the bulk write API.
    """
    def __init__(self, collection, ordered, bypass_document_validation):
        """Initialize a _Bulk instance.
        """
        self.collection = collection.with_options(
            codec_options=collection.codec_options._replace(
                unicode_decode_error_handler='replace',
                document_class=dict))
        self.ordered = ordered
        self.ops = []
        self.executed = False
        self.bypass_doc_val = bypass_document_validation
        self.uses_collation = False
        self.uses_array_filters = False
        self.is_retryable = True
        self.retrying = False
        self.started_retryable_write = False
        # Extra state so that we know where to pick up on a retry attempt.
        self.current_run = None

    @property
    def bulk_ctx_class(self):
        encrypter = self.collection.database.client._encrypter
        if encrypter and not encrypter._bypass_auto_encryption:
            return _EncryptedBulkWriteContext
        else:
            return _BulkWriteContext

    def add_insert(self, document):
        """Add an insert document to the list of ops.
        """
        validate_is_document_type("document", document)
        # Generate ObjectId client side.
        if not (isinstance(document, RawBSONDocument) or '_id' in document):
            document['_id'] = ObjectId()
        self.ops.append((_INSERT, document))

    def add_update(self, selector, update, multi=False, upsert=False,
                   collation=None, array_filters=None):
        """Create an update document and add it to the list of ops.
        """
        validate_ok_for_update(update)
        cmd = SON([('q', selector), ('u', update),
                   ('multi', multi), ('upsert', upsert)])
        collation = validate_collation_or_none(collation)
        if collation is not None:
            self.uses_collation = True
            cmd['collation'] = collation
        if array_filters is not None:
            self.uses_array_filters = True
            cmd['arrayFilters'] = array_filters
        if multi:
            # A bulk_write containing an update_many is not retryable.
            self.is_retryable = False
        self.ops.append((_UPDATE, cmd))

    def add_replace(self, selector, replacement, upsert=False,
                    collation=None):
        """Create a replace document and add it to the list of ops.
        """
        validate_ok_for_replace(replacement)
        cmd = SON([('q', selector), ('u', replacement),
                   ('multi', False), ('upsert', upsert)])
        collation = validate_collation_or_none(collation)
        if collation is not None:
            self.uses_collation = True
            cmd['collation'] = collation
        self.ops.append((_UPDATE, cmd))

    def add_delete(self, selector, limit, collation=None):
        """Create a delete document and add it to the list of ops.
        """
        cmd = SON([('q', selector), ('limit', limit)])
        collation = validate_collation_or_none(collation)
        if collation is not None:
            self.uses_collation = True
            cmd['collation'] = collation
        if limit == _DELETE_ALL:
            # A bulk_write containing a delete_many is not retryable.
            self.is_retryable = False
        self.ops.append((_DELETE, cmd))

    def gen_ordered(self):
        """Generate batches of operations, batched by type of
        operation, in the order **provided**.
        """
        run = None
        for idx, (op_type, operation) in enumerate(self.ops):
            if run is None:
                run = _Run(op_type)
            elif run.op_type != op_type:
                yield run
                run = _Run(op_type)
            run.add(idx, operation)
        yield run

    def gen_unordered(self):
        """Generate batches of operations, batched by type of
        operation, in arbitrary order.
        """
        operations = [_Run(_INSERT), _Run(_UPDATE), _Run(_DELETE)]
        for idx, (op_type, operation) in enumerate(self.ops):
            operations[op_type].add(idx, operation)

        for run in operations:
            if run.ops:
                yield run

    def _execute_command(self, generator, write_concern, session,
                         sock_info, op_id, retryable, full_result):
        if sock_info.max_wire_version < 5 and self.uses_collation:
            raise ConfigurationError(
                'Must be connected to MongoDB 3.4+ to use a collation.')
        if sock_info.max_wire_version < 6 and self.uses_array_filters:
            raise ConfigurationError(
                'Must be connected to MongoDB 3.6+ to use arrayFilters.')

        db_name = self.collection.database.name
        client = self.collection.database.client
        listeners = client._event_listeners

        if not self.current_run:
            self.current_run = next(generator)
        run = self.current_run

        # sock_info.command validates the session, but we use
        # sock_info.write_command.
        sock_info.validate_session(client, session)
        while run:
            cmd = SON([(_COMMANDS[run.op_type], self.collection.name),
                       ('ordered', self.ordered)])
            if not write_concern.is_server_default:
                cmd['writeConcern'] = write_concern.document
            if self.bypass_doc_val and sock_info.max_wire_version >= 4:
                cmd['bypassDocumentValidation'] = True
            bwc = self.bulk_ctx_class(
                db_name, cmd, sock_info, op_id, listeners, session,
                run.op_type, self.collection.codec_options)

            while run.idx_offset < len(run.ops):
                if session:
                    # Start a new retryable write unless one was already
                    # started for this command.
                    if retryable and not self.started_retryable_write:
                        session._start_retryable_write()
                        self.started_retryable_write = True
                    session._apply_to(cmd, retryable, ReadPreference.PRIMARY)
                sock_info.send_cluster_time(cmd, session, client)
                ops = islice(run.ops, run.idx_offset, None)
                # Run as many ops as possible in one command.
                result, to_send = bwc.execute(ops, client)

                # Retryable writeConcernErrors halt the execution of this run.
                wce = result.get('writeConcernError', {})
                if wce.get('code', 0) in _RETRYABLE_ERROR_CODES:
                    # Synthesize the full bulk result without modifying the
                    # current one because this write operation may be retried.
                    full = copy.deepcopy(full_result)
                    _merge_command(run, full, run.idx_offset, result)
                    _raise_bulk_write_error(full)

                _merge_command(run, full_result, run.idx_offset, result)
                # We're no longer in a retry once a command succeeds.
                self.retrying = False
                self.started_retryable_write = False

                if self.ordered and "writeErrors" in result:
                    break
                run.idx_offset += len(to_send)

            # We're supposed to continue if errors are
            # at the write concern level (e.g. wtimeout)
            if self.ordered and full_result['writeErrors']:
                break
            # Reset our state
            self.current_run = run = next(generator, None)

    def execute_command(self, generator, write_concern, session):
        """Execute using write commands.
        """
        # nModified is only reported for write commands, not legacy ops.
        full_result = {
            "writeErrors": [],
            "writeConcernErrors": [],
            "nInserted": 0,
            "nUpserted": 0,
            "nMatched": 0,
            "nModified": 0,
            "nRemoved": 0,
            "upserted": [],
        }
        op_id = _randint()

        def retryable_bulk(session, sock_info, retryable):
            self._execute_command(
                generator, write_concern, session, sock_info, op_id,
                retryable, full_result)

        client = self.collection.database.client
        with client._tmp_session(session) as s:
            client._retry_with_session(
                self.is_retryable, retryable_bulk, s, self)

        if full_result["writeErrors"] or full_result["writeConcernErrors"]:
            _raise_bulk_write_error(full_result)
        return full_result

    def execute_insert_no_results(self, sock_info, run, op_id, acknowledged):
        """Execute insert, returning no results.
        """
        command = SON([('insert', self.collection.name),
                       ('ordered', self.ordered)])
        concern = {'w': int(self.ordered)}
        command['writeConcern'] = concern
        if self.bypass_doc_val and sock_info.max_wire_version >= 4:
            command['bypassDocumentValidation'] = True
        db = self.collection.database
        bwc = _BulkWriteContext(
            db.name, command, sock_info, op_id, db.client._event_listeners,
            None, _INSERT, self.collection.codec_options)
        # Legacy batched OP_INSERT.
        _do_batched_insert(
            self.collection.full_name, run.ops, True, acknowledged, concern,
            not self.ordered, self.collection.codec_options, bwc)

    def execute_op_msg_no_results(self, sock_info, generator):
        """Execute write commands with OP_MSG and w=0 writeConcern, unordered.
        """
        db_name = self.collection.database.name
        client = self.collection.database.client
        listeners = client._event_listeners
        op_id = _randint()

        if not self.current_run:
            self.current_run = next(generator)
        run = self.current_run

        while run:
            cmd = SON([(_COMMANDS[run.op_type], self.collection.name),
                       ('ordered', False),
                       ('writeConcern', {'w': 0})])
            bwc = self.bulk_ctx_class(
                db_name, cmd, sock_info, op_id, listeners, None,
                run.op_type, self.collection.codec_options)

            while run.idx_offset < len(run.ops):
                ops = islice(run.ops, run.idx_offset, None)
                # Run as many ops as possible.
                to_send = bwc.execute_unack(ops, client)
                run.idx_offset += len(to_send)
            self.current_run = run = next(generator, None)

    def execute_command_no_results(self, sock_info, generator):
        """Execute write commands with OP_MSG and w=0 WriteConcern, ordered.
        """
        full_result = {
            "writeErrors": [],
            "writeConcernErrors": [],
            "nInserted": 0,
            "nUpserted": 0,
            "nMatched": 0,
            "nModified": 0,
            "nRemoved": 0,
            "upserted": [],
        }
        # Ordered bulk writes have to be acknowledged so that we stop
        # processing at the first error, even when the application
        # specified unacknowledged writeConcern.
        write_concern = WriteConcern()
        op_id = _randint()
        try:
            self._execute_command(
                generator, write_concern, None,
                sock_info, op_id, False, full_result)
        except OperationFailure:
            pass

    def execute_no_results(self, sock_info, generator):
        """Execute all operations, returning no results (w=0).
        """
        if self.uses_collation:
            raise ConfigurationError(
                'Collation is unsupported for unacknowledged writes.')
        if self.uses_array_filters:
            raise ConfigurationError(
                'arrayFilters is unsupported for unacknowledged writes.')
        # Cannot have both unacknowledged writes and bypass document validation.
        if self.bypass_doc_val and sock_info.max_wire_version >= 4:
            raise OperationFailure("Cannot set bypass_document_validation with"
                                   " unacknowledged write concern")

        # OP_MSG
        if sock_info.max_wire_version > 5:
            if self.ordered:
                return self.execute_command_no_results(sock_info, generator)
            return self.execute_op_msg_no_results(sock_info, generator)

        coll = self.collection
        # If ordered is True we have to send GLE or use write
        # commands so we can abort on the first error.
        write_concern = WriteConcern(w=int(self.ordered))
        op_id = _randint()

        next_run = next(generator)
        while next_run:
            # An ordered bulk write needs to send acknowledged writes to short
            # circuit the next run. However, the final message on the final
            # run can be unacknowledged.
            run = next_run
            next_run = next(generator, None)
            needs_ack = self.ordered and next_run is not None
            try:
                if run.op_type == _INSERT:
                    self.execute_insert_no_results(
                        sock_info, run, op_id, needs_ack)
                elif run.op_type == _UPDATE:
                    for operation in run.ops:
                        doc = operation['u']
                        check_keys = True
                        if doc and next(iter(doc)).startswith('$'):
                            check_keys = False
                        coll._update(
                            sock_info,
                            operation['q'],
                            doc,
                            operation['upsert'],
                            check_keys,
                            operation['multi'],
                            write_concern=write_concern,
                            op_id=op_id,
                            ordered=self.ordered,
                            bypass_doc_val=self.bypass_doc_val)
                else:
                    for operation in run.ops:
                        coll._delete(sock_info,
                                     operation['q'],
                                     not operation['limit'],
                                     write_concern,
                                     op_id,
                                     self.ordered)
            except OperationFailure:
                if self.ordered:
                    break

    def execute(self, write_concern, session):
        """Execute operations.
        """
        if not self.ops:
            raise InvalidOperation('No operations to execute')
        if self.executed:
            raise InvalidOperation('Bulk operations can '
                                   'only be executed once.')
        self.executed = True
        write_concern = write_concern or self.collection.write_concern
        session = _validate_session_write_concern(session, write_concern)

        if self.ordered:
            generator = self.gen_ordered()
        else:
            generator = self.gen_unordered()

        client = self.collection.database.client
        if not write_concern.acknowledged:
            with client._socket_for_writes(session) as sock_info:
                self.execute_no_results(sock_info, generator)
        else:
            return self.execute_command(generator, write_concern, session)


class BulkUpsertOperation(object):
    """An interface for adding upsert operations.
    """

    __slots__ = ('__selector', '__bulk', '__collation')

    def __init__(self, selector, bulk, collation):
        self.__selector = selector
        self.__bulk = bulk
        self.__collation = collation

    def update_one(self, update):
        """Update one document matching the selector.

        :Parameters:
          - `update` (dict): the update operations to apply
        """
        self.__bulk.add_update(self.__selector,
                               update, multi=False, upsert=True,
                               collation=self.__collation)

    def update(self, update):
        """Update all documents matching the selector.

        :Parameters:
          - `update` (dict): the update operations to apply
        """
        self.__bulk.add_update(self.__selector,
                               update, multi=True, upsert=True,
                               collation=self.__collation)

    def replace_one(self, replacement):
        """Replace one entire document matching the selector criteria.

        :Parameters:
          - `replacement` (dict): the replacement document
        """
        self.__bulk.add_replace(self.__selector, replacement, upsert=True,
                                collation=self.__collation)


class BulkWriteOperation(object):
    """An interface for adding update or remove operations.
    """

    __slots__ = ('__selector', '__bulk', '__collation')

    def __init__(self, selector, bulk, collation):
        self.__selector = selector
        self.__bulk = bulk
        self.__collation = collation

    def update_one(self, update):
        """Update one document matching the selector criteria.

        :Parameters:
          - `update` (dict): the update operations to apply
        """
        self.__bulk.add_update(self.__selector, update, multi=False,
                               collation=self.__collation)

    def update(self, update):
        """Update all documents matching the selector criteria.

        :Parameters:
          - `update` (dict): the update operations to apply
        """
        self.__bulk.add_update(self.__selector, update, multi=True,
                               collation=self.__collation)

    def replace_one(self, replacement):
        """Replace one entire document matching the selector criteria.

        :Parameters:
          - `replacement` (dict): the replacement document
        """
        self.__bulk.add_replace(self.__selector, replacement,
                                collation=self.__collation)

    def remove_one(self):
        """Remove a single document matching the selector criteria.
        """
        self.__bulk.add_delete(self.__selector, _DELETE_ONE,
                               collation=self.__collation)

    def remove(self):
        """Remove all documents matching the selector criteria.
        """
        self.__bulk.add_delete(self.__selector, _DELETE_ALL,
                               collation=self.__collation)

    def upsert(self):
        """Specify that all chained update operations should be
        upserts.

        :Returns:
          - A :class:`BulkUpsertOperation` instance, used to add
            update operations to this bulk operation.
        """
        return BulkUpsertOperation(self.__selector, self.__bulk,
                                   self.__collation)


class BulkOperationBuilder(object):
    """**DEPRECATED**: An interface for executing a batch of write operations.
    """

    __slots__ = '__bulk'

    def __init__(self, collection, ordered=True,
                 bypass_document_validation=False):
        """**DEPRECATED**: Initialize a new BulkOperationBuilder instance.

        :Parameters:
          - `collection`: A :class:`~pymongo.collection.Collection` instance.
          - `ordered` (optional): If ``True`` all operations will be executed
            serially, in the order provided, and the entire execution will
            abort on the first error. If ``False`` operations will be executed
            in arbitrary order (possibly in parallel on the server), reporting
            any errors that occurred after attempting all operations. Defaults
            to ``True``.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 3.5
           Deprecated. Use :meth:`~pymongo.collection.Collection.bulk_write`
           instead.

        .. versionchanged:: 3.2
          Added bypass_document_validation support
        """
        self.__bulk = _Bulk(collection, ordered, bypass_document_validation)

    def find(self, selector, collation=None):
        """Specify selection criteria for bulk operations.

        :Parameters:
          - `selector` (dict): the selection criteria for update
            and remove operations.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only
            supported on MongoDB 3.4 and above.

        :Returns:
          - A :class:`BulkWriteOperation` instance, used to add
            update and remove operations to this bulk operation.

        .. versionchanged:: 3.4
           Added the `collation` option.

        """
        validate_is_mapping("selector", selector)
        return BulkWriteOperation(selector, self.__bulk, collation)

    def insert(self, document):
        """Insert a single document.

        :Parameters:
          - `document` (dict): the document to insert

        .. seealso:: :ref:`writes-and-ids`
        """
        self.__bulk.add_insert(document)

    def execute(self, write_concern=None):
        """Execute all provided operations.

        :Parameters:
          - write_concern (optional): the write concern for this bulk
            execution.
        """
        if write_concern is not None:
            write_concern = WriteConcern(**write_concern)
        return self.__bulk.execute(write_concern, session=None)
