# Copyright 2015-present MongoDB, Inc.
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

"""Internal network layer helper methods."""

import datetime
import errno
import select
import struct
import threading

_HAS_POLL = True
_EVENT_MASK = 0
try:
    from select import poll
    _EVENT_MASK = (
        select.POLLIN | select.POLLPRI | select.POLLERR | select.POLLHUP)
except ImportError:
    _HAS_POLL = False

try:
    from select import error as _SELECT_ERROR
except ImportError:
    _SELECT_ERROR = OSError

from bson import _decode_all_selective
from bson.py3compat import PY3

from pymongo import helpers, message
from pymongo.common import MAX_MESSAGE_SIZE
from pymongo.compression_support import decompress, _NO_COMPRESSION
from pymongo.errors import (AutoReconnect,
                            NotMasterError,
                            OperationFailure,
                            ProtocolError)
from pymongo.message import _UNPACK_REPLY


_UNPACK_HEADER = struct.Struct("<iiii").unpack


def command(sock, dbname, spec, slave_ok, is_mongos,
            read_preference, codec_options, session, client, check=True,
            allowable_errors=None, address=None,
            check_keys=False, listeners=None, max_bson_size=None,
            read_concern=None,
            parse_write_concern_error=False,
            collation=None,
            compression_ctx=None,
            use_op_msg=False,
            unacknowledged=False,
            user_fields=None):
    """Execute a command over the socket, or raise socket.error.

    :Parameters:
      - `sock`: a raw socket instance
      - `dbname`: name of the database on which to run the command
      - `spec`: a command document as an ordered dict type, eg SON.
      - `slave_ok`: whether to set the SlaveOkay wire protocol bit
      - `is_mongos`: are we connected to a mongos?
      - `read_preference`: a read preference
      - `codec_options`: a CodecOptions instance
      - `session`: optional ClientSession instance.
      - `client`: optional MongoClient instance for updating $clusterTime.
      - `check`: raise OperationFailure if there are errors
      - `allowable_errors`: errors to ignore if `check` is True
      - `address`: the (host, port) of `sock`
      - `check_keys`: if True, check `spec` for invalid keys
      - `listeners`: An instance of :class:`~pymongo.monitoring.EventListeners`
      - `max_bson_size`: The maximum encoded bson size for this server
      - `read_concern`: The read concern for this command.
      - `parse_write_concern_error`: Whether to parse the ``writeConcernError``
        field in the command response.
      - `collation`: The collation for this command.
      - `compression_ctx`: optional compression Context.
      - `use_op_msg`: True if we should use OP_MSG.
      - `unacknowledged`: True if this is an unacknowledged command.
      - `user_fields` (optional): Response fields that should be decoded
        using the TypeDecoders from codec_options, passed to
        bson._decode_all_selective.
    """
    name = next(iter(spec))
    ns = dbname + '.$cmd'
    flags = 4 if slave_ok else 0

    # Publish the original command document, perhaps with lsid and $clusterTime.
    orig = spec
    if is_mongos and not use_op_msg:
        spec = message._maybe_add_read_preference(spec, read_preference)
    if read_concern and not (session and session._in_transaction):
        if read_concern.level:
            spec['readConcern'] = read_concern.document
        if (session and session.options.causal_consistency
                and session.operation_time is not None):
            spec.setdefault(
                'readConcern', {})['afterClusterTime'] = session.operation_time
    if collation is not None:
        spec['collation'] = collation

    publish = listeners is not None and listeners.enabled_for_commands
    if publish:
        start = datetime.datetime.now()

    if compression_ctx and name.lower() in _NO_COMPRESSION:
        compression_ctx = None

    if (client and client._encrypter and
            not client._encrypter._bypass_auto_encryption):
        spec = orig = client._encrypter.encrypt(
            dbname, spec, check_keys, codec_options)
        # We already checked the keys, no need to do it again.
        check_keys = False

    if use_op_msg:
        flags = 2 if unacknowledged else 0
        request_id, msg, size, max_doc_size = message._op_msg(
            flags, spec, dbname, read_preference, slave_ok, check_keys,
            codec_options, ctx=compression_ctx)
        # If this is an unacknowledged write then make sure the encoded doc(s)
        # are small enough, otherwise rely on the server to return an error.
        if (unacknowledged and max_bson_size is not None and
                max_doc_size > max_bson_size):
            message._raise_document_too_large(name, size, max_bson_size)
    else:
        request_id, msg, size = message.query(
            flags, ns, 0, -1, spec, None, codec_options, check_keys,
            compression_ctx)

    if (max_bson_size is not None
            and size > max_bson_size + message._COMMAND_OVERHEAD):
        message._raise_document_too_large(
            name, size, max_bson_size + message._COMMAND_OVERHEAD)

    if publish:
        encoding_duration = datetime.datetime.now() - start
        listeners.publish_command_start(orig, dbname, request_id, address)
        start = datetime.datetime.now()

    try:
        sock.sendall(msg)
        if use_op_msg and unacknowledged:
            # Unacknowledged, fake a successful command response.
            reply = None
            response_doc = {"ok": 1}
        else:
            reply = receive_message(sock, request_id)
            unpacked_docs = reply.unpack_response(
                codec_options=codec_options, user_fields=user_fields)

            response_doc = unpacked_docs[0]
            if client:
                client._process_response(response_doc, session)
            if check:
                helpers._check_command_response(
                    response_doc, None, allowable_errors,
                    parse_write_concern_error=parse_write_concern_error)
    except Exception as exc:
        if publish:
            duration = (datetime.datetime.now() - start) + encoding_duration
            if isinstance(exc, (NotMasterError, OperationFailure)):
                failure = exc.details
            else:
                failure = message._convert_exception(exc)
            listeners.publish_command_failure(
                duration, failure, name, request_id, address)
        raise
    if publish:
        duration = (datetime.datetime.now() - start) + encoding_duration
        listeners.publish_command_success(
            duration, response_doc, name, request_id, address)

    if client and client._encrypter and reply:
        decrypted = client._encrypter.decrypt(reply.raw_command_response())
        response_doc = _decode_all_selective(decrypted, codec_options,
                                             user_fields)[0]

    return response_doc

_UNPACK_COMPRESSION_HEADER = struct.Struct("<iiB").unpack

def receive_message(sock, request_id, max_message_size=MAX_MESSAGE_SIZE):
    """Receive a raw BSON message or raise socket.error."""
    # Ignore the response's request id.
    length, _, response_to, op_code = _UNPACK_HEADER(
        _receive_data_on_socket(sock, 16))
    # No request_id for exhaust cursor "getMore".
    if request_id is not None:
        if request_id != response_to:
            raise ProtocolError("Got response id %r but expected "
                                "%r" % (response_to, request_id))
    if length <= 16:
        raise ProtocolError("Message length (%r) not longer than standard "
                            "message header size (16)" % (length,))
    if length > max_message_size:
        raise ProtocolError("Message length (%r) is larger than server max "
                            "message size (%r)" % (length, max_message_size))
    if op_code == 2012:
        op_code, _, compressor_id = _UNPACK_COMPRESSION_HEADER(
            _receive_data_on_socket(sock, 9))
        data = decompress(
            _receive_data_on_socket(sock, length - 25), compressor_id)
    else:
        data = _receive_data_on_socket(sock, length - 16)

    try:
        unpack_reply = _UNPACK_REPLY[op_code]
    except KeyError:
        raise ProtocolError("Got opcode %r but expected "
                            "%r" % (op_code, _UNPACK_REPLY.keys()))
    return unpack_reply(data)


# memoryview was introduced in Python 2.7 but we only use it on Python 3
# because before 2.7.4 the struct module did not support memoryview:
# https://bugs.python.org/issue10212.
# In Jython, using slice assignment on a memoryview results in a
# NullPointerException.
if not PY3:
    def _receive_data_on_socket(sock, length):
        buf = bytearray(length)
        i = 0
        while length:
            try:
                chunk = sock.recv(length)
            except (IOError, OSError) as exc:
                if _errno_from_exception(exc) == errno.EINTR:
                    continue
                raise
            if chunk == b"":
                raise AutoReconnect("connection closed")

            buf[i:i + len(chunk)] = chunk
            i += len(chunk)
            length -= len(chunk)

        return bytes(buf)
else:
    def _receive_data_on_socket(sock, length):
        buf = bytearray(length)
        mv = memoryview(buf)
        bytes_read = 0
        while bytes_read < length:
            try:
                chunk_length = sock.recv_into(mv[bytes_read:])
            except (IOError, OSError) as exc:
                if _errno_from_exception(exc) == errno.EINTR:
                    continue
                raise
            if chunk_length == 0:
                raise AutoReconnect("connection closed")

            bytes_read += chunk_length

        return mv


def _errno_from_exception(exc):
    if hasattr(exc, 'errno'):
        return exc.errno
    elif exc.args:
        return exc.args[0]
    else:
        return None


class SocketChecker(object):

    def __init__(self):
        if _HAS_POLL:
            self._lock = threading.Lock()
            self._poller = poll()
        else:
            self._lock = None
            self._poller = None

    def socket_closed(self, sock):
        """Return True if we know socket has been closed, False otherwise.
        """
        while True:
            try:
                if self._poller:
                    with self._lock:
                        self._poller.register(sock, _EVENT_MASK)
                        try:
                            rd = self._poller.poll(0)
                        finally:
                            self._poller.unregister(sock)
                else:
                    rd, _, _ = select.select([sock], [], [], 0)
            except (RuntimeError, KeyError):
                # RuntimeError is raised during a concurrent poll. KeyError
                # is raised by unregister if the socket is not in the poller.
                # These errors should not be possible since we protect the
                # poller with a mutex.
                raise
            except ValueError:
                # ValueError is raised by register/unregister/select if the
                # socket file descriptor is negative or outside the range for
                # select (> 1023).
                return True
            except (_SELECT_ERROR, IOError) as exc:
                if _errno_from_exception(exc) in (errno.EINTR, errno.EAGAIN):
                    continue
                return True
            except Exception:
                # Any other exceptions should be attributed to a closed
                # or invalid socket.
                return True
            return len(rd) > 0
