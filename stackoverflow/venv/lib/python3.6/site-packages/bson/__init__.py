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

"""BSON (Binary JSON) encoding and decoding.

The mapping from Python types to BSON types is as follows:

=======================================  =============  ===================
Python Type                              BSON Type      Supported Direction
=======================================  =============  ===================
None                                     null           both
bool                                     boolean        both
int [#int]_                              int32 / int64  py -> bson
long                                     int64          py -> bson
`bson.int64.Int64`                       int64          both
float                                    number (real)  both
string                                   string         py -> bson
unicode                                  string         both
list                                     array          both
dict / `SON`                             object         both
datetime.datetime [#dt]_ [#dt2]_         date           both
`bson.regex.Regex`                       regex          both
compiled re [#re]_                       regex          py -> bson
`bson.binary.Binary`                     binary         both
`bson.objectid.ObjectId`                 oid            both
`bson.dbref.DBRef`                       dbref          both
None                                     undefined      bson -> py
unicode                                  code           bson -> py
`bson.code.Code`                         code           py -> bson
unicode                                  symbol         bson -> py
bytes (Python 3) [#bytes]_               binary         both
=======================================  =============  ===================

Note that, when using Python 2.x, to save binary data it must be wrapped as
an instance of `bson.binary.Binary`. Otherwise it will be saved as a BSON
string and retrieved as unicode. Users of Python 3.x can use the Python bytes
type.

.. [#int] A Python int will be saved as a BSON int32 or BSON int64 depending
   on its size. A BSON int32 will always decode to a Python int. A BSON
   int64 will always decode to a :class:`~bson.int64.Int64`.
.. [#dt] datetime.datetime instances will be rounded to the nearest
   millisecond when saved
.. [#dt2] all datetime.datetime instances are treated as *naive*. clients
   should always use UTC.
.. [#re] :class:`~bson.regex.Regex` instances and regular expression
   objects from ``re.compile()`` are both saved as BSON regular expressions.
   BSON regular expressions are decoded as :class:`~bson.regex.Regex`
   instances.
.. [#bytes] The bytes type from Python 3.x is encoded as BSON binary with
   subtype 0. In Python 3.x it will be decoded back to bytes. In Python 2.x
   it will be decoded to an instance of :class:`~bson.binary.Binary` with
   subtype 0.
"""

import calendar
import datetime
import itertools
import platform
import re
import struct
import sys
import uuid

from codecs import (utf_8_decode as _utf_8_decode,
                    utf_8_encode as _utf_8_encode)

from bson.binary import (Binary, OLD_UUID_SUBTYPE,
                         JAVA_LEGACY, CSHARP_LEGACY,
                         UUIDLegacy)
from bson.code import Code
from bson.codec_options import (
    CodecOptions, DEFAULT_CODEC_OPTIONS, _raw_document_class)
from bson.dbref import DBRef
from bson.decimal128 import Decimal128
from bson.errors import (InvalidBSON,
                         InvalidDocument,
                         InvalidStringData)
from bson.int64 import Int64
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.py3compat import (abc,
                            b,
                            PY3,
                            iteritems,
                            text_type,
                            string_type,
                            reraise)
from bson.regex import Regex
from bson.son import SON, RE_TYPE
from bson.timestamp import Timestamp
from bson.tz_util import utc


try:
    from bson import _cbson
    _USE_C = True
except ImportError:
    _USE_C = False


EPOCH_AWARE = datetime.datetime.fromtimestamp(0, utc)
EPOCH_NAIVE = datetime.datetime.utcfromtimestamp(0)


BSONNUM = b"\x01" # Floating point
BSONSTR = b"\x02" # UTF-8 string
BSONOBJ = b"\x03" # Embedded document
BSONARR = b"\x04" # Array
BSONBIN = b"\x05" # Binary
BSONUND = b"\x06" # Undefined
BSONOID = b"\x07" # ObjectId
BSONBOO = b"\x08" # Boolean
BSONDAT = b"\x09" # UTC Datetime
BSONNUL = b"\x0A" # Null
BSONRGX = b"\x0B" # Regex
BSONREF = b"\x0C" # DBRef
BSONCOD = b"\x0D" # Javascript code
BSONSYM = b"\x0E" # Symbol
BSONCWS = b"\x0F" # Javascript code with scope
BSONINT = b"\x10" # 32bit int
BSONTIM = b"\x11" # Timestamp
BSONLON = b"\x12" # 64bit int
BSONDEC = b"\x13" # Decimal128
BSONMIN = b"\xFF" # Min key
BSONMAX = b"\x7F" # Max key


_UNPACK_FLOAT_FROM = struct.Struct("<d").unpack_from
_UNPACK_INT = struct.Struct("<i").unpack
_UNPACK_INT_FROM = struct.Struct("<i").unpack_from
_UNPACK_LENGTH_SUBTYPE_FROM = struct.Struct("<iB").unpack_from
_UNPACK_LONG_FROM = struct.Struct("<q").unpack_from
_UNPACK_TIMESTAMP_FROM = struct.Struct("<II").unpack_from


if PY3:
    _OBJEND = 0
    # Only used to generate the _ELEMENT_GETTER dict
    def _maybe_ord(element_type):
        return ord(element_type)
    # Only used in _raise_unkown_type below
    def _elt_to_hex(element_type):
        return chr(element_type).encode()
    _supported_buffer_types = (bytes, bytearray)
else:
    _OBJEND = b"\x00"
    def _maybe_ord(element_type):
        return element_type
    def _elt_to_hex(element_type):
        return element_type
    _supported_buffer_types = (bytes,)



if platform.python_implementation() == 'Jython':
    # This is why we can't have nice things.
    # https://bugs.jython.org/issue2788
    def get_data_and_view(data):
        if isinstance(data, _supported_buffer_types):
            return data, data
        data = memoryview(data).tobytes()
        return data, data
else:
    def get_data_and_view(data):
        if isinstance(data, _supported_buffer_types):
            return data, memoryview(data)
        view = memoryview(data)
        return view.tobytes(), view


def _raise_unknown_type(element_type, element_name):
    """Unknown type helper."""
    raise InvalidBSON("Detected unknown BSON type %r for fieldname '%s'. Are "
                      "you using the latest driver version?" % (
                          _elt_to_hex(element_type), element_name))


def _get_int(data, view, position, dummy0, dummy1, dummy2):
    """Decode a BSON int32 to python int."""
    return _UNPACK_INT_FROM(data, position)[0], position + 4


def _get_c_string(data, view, position, opts):
    """Decode a BSON 'C' string to python unicode string."""
    end = data.index(b"\x00", position)
    return _utf_8_decode(view[position:end],
                         opts.unicode_decode_error_handler, True)[0], end + 1


def _get_float(data, view, position, dummy0, dummy1, dummy2):
    """Decode a BSON double to python float."""
    return _UNPACK_FLOAT_FROM(data, position)[0], position + 8


def _get_string(data, view, position, obj_end, opts, dummy):
    """Decode a BSON string to python unicode string."""
    length = _UNPACK_INT_FROM(data, position)[0]
    position += 4
    if length < 1 or obj_end - position < length:
        raise InvalidBSON("invalid string length")
    end = position + length - 1
    if data[end] != _OBJEND:
        raise InvalidBSON("invalid end of string")
    return _utf_8_decode(view[position:end],
                         opts.unicode_decode_error_handler, True)[0], end + 1


def _get_object_size(data, position, obj_end):
    """Validate and return a BSON document's size."""
    try:
        obj_size = _UNPACK_INT_FROM(data, position)[0]
    except struct.error as exc:
        raise InvalidBSON(str(exc))
    end = position + obj_size - 1
    if data[end] != _OBJEND:
        raise InvalidBSON("bad eoo")
    if end >= obj_end:
        raise InvalidBSON("invalid object length")
    # If this is the top-level document, validate the total size too.
    if position == 0 and obj_size != obj_end:
        raise InvalidBSON("invalid object length")
    return obj_size, end


def _get_object(data, view, position, obj_end, opts, dummy):
    """Decode a BSON subdocument to opts.document_class or bson.dbref.DBRef."""
    obj_size, end = _get_object_size(data, position, obj_end)
    if _raw_document_class(opts.document_class):
        return (opts.document_class(data[position:end + 1], opts),
                position + obj_size)

    obj = _elements_to_dict(data, view, position + 4, end, opts)

    position += obj_size
    if "$ref" in obj:
        return (DBRef(obj.pop("$ref"), obj.pop("$id", None),
                      obj.pop("$db", None), obj), position)
    return obj, position


def _get_array(data, view, position, obj_end, opts, element_name):
    """Decode a BSON array to python list."""
    size = _UNPACK_INT_FROM(data, position)[0]
    end = position + size - 1
    if data[end] != _OBJEND:
        raise InvalidBSON("bad eoo")

    position += 4
    end -= 1
    result = []

    # Avoid doing global and attribute lookups in the loop.
    append = result.append
    index = data.index
    getter = _ELEMENT_GETTER
    decoder_map = opts.type_registry._decoder_map

    while position < end:
        element_type = data[position]
        # Just skip the keys.
        position = index(b'\x00', position) + 1
        try:
            value, position = getter[element_type](
                data, view, position, obj_end, opts, element_name)
        except KeyError:
            _raise_unknown_type(element_type, element_name)

        if decoder_map:
            custom_decoder = decoder_map.get(type(value))
            if custom_decoder is not None:
                value = custom_decoder(value)

        append(value)

    if position != end + 1:
        raise InvalidBSON('bad array length')
    return result, position + 1


def _get_binary(data, view, position, obj_end, opts, dummy1):
    """Decode a BSON binary to bson.binary.Binary or python UUID."""
    length, subtype = _UNPACK_LENGTH_SUBTYPE_FROM(data, position)
    position += 5
    if subtype == 2:
        length2 = _UNPACK_INT_FROM(data, position)[0]
        position += 4
        if length2 != length - 4:
            raise InvalidBSON("invalid binary (st 2) - lengths don't match!")
        length = length2
    end = position + length
    if length < 0 or end > obj_end:
        raise InvalidBSON('bad binary object length')
    if subtype == 3:
        # Java Legacy
        uuid_representation = opts.uuid_representation
        if uuid_representation == JAVA_LEGACY:
            java = data[position:end]
            value = uuid.UUID(bytes=java[0:8][::-1] + java[8:16][::-1])
        # C# legacy
        elif uuid_representation == CSHARP_LEGACY:
            value = uuid.UUID(bytes_le=data[position:end])
        # Python
        else:
            value = uuid.UUID(bytes=data[position:end])
        return value, end
    if subtype == 4:
        return uuid.UUID(bytes=data[position:end]), end
    # Python3 special case. Decode subtype 0 to 'bytes'.
    if PY3 and subtype == 0:
        value = data[position:end]
    else:
        value = Binary(data[position:end], subtype)
    return value, end


def _get_oid(data, view, position, dummy0, dummy1, dummy2):
    """Decode a BSON ObjectId to bson.objectid.ObjectId."""
    end = position + 12
    return ObjectId(data[position:end]), end


def _get_boolean(data, view, position, dummy0, dummy1, dummy2):
    """Decode a BSON true/false to python True/False."""
    end = position + 1
    boolean_byte = data[position:end]
    if boolean_byte == b'\x00':
        return False, end
    elif boolean_byte == b'\x01':
        return True, end
    raise InvalidBSON('invalid boolean value: %r' % boolean_byte)


def _get_date(data, view, position, dummy0, opts, dummy1):
    """Decode a BSON datetime to python datetime.datetime."""
    return _millis_to_datetime(
        _UNPACK_LONG_FROM(data, position)[0], opts), position + 8


def _get_code(data, view, position, obj_end, opts, element_name):
    """Decode a BSON code to bson.code.Code."""
    code, position = _get_string(data, view, position, obj_end, opts, element_name)
    return Code(code), position


def _get_code_w_scope(data, view, position, obj_end, opts, element_name):
    """Decode a BSON code_w_scope to bson.code.Code."""
    code_end = position + _UNPACK_INT_FROM(data, position)[0]
    code, position = _get_string(
        data, view, position + 4, code_end, opts, element_name)
    scope, position = _get_object(data, view, position, code_end, opts, element_name)
    if position != code_end:
        raise InvalidBSON('scope outside of javascript code boundaries')
    return Code(code, scope), position


def _get_regex(data, view, position, dummy0, opts, dummy1):
    """Decode a BSON regex to bson.regex.Regex or a python pattern object."""
    pattern, position = _get_c_string(data, view, position, opts)
    bson_flags, position = _get_c_string(data, view, position, opts)
    bson_re = Regex(pattern, bson_flags)
    return bson_re, position


def _get_ref(data, view, position, obj_end, opts, element_name):
    """Decode (deprecated) BSON DBPointer to bson.dbref.DBRef."""
    collection, position = _get_string(
        data, view, position, obj_end, opts, element_name)
    oid, position = _get_oid(data, view, position, obj_end, opts, element_name)
    return DBRef(collection, oid), position


def _get_timestamp(data, view, position, dummy0, dummy1, dummy2):
    """Decode a BSON timestamp to bson.timestamp.Timestamp."""
    inc, timestamp = _UNPACK_TIMESTAMP_FROM(data, position)
    return Timestamp(timestamp, inc), position + 8


def _get_int64(data, view, position, dummy0, dummy1, dummy2):
    """Decode a BSON int64 to bson.int64.Int64."""
    return Int64(_UNPACK_LONG_FROM(data, position)[0]), position + 8


def _get_decimal128(data, view, position, dummy0, dummy1, dummy2):
    """Decode a BSON decimal128 to bson.decimal128.Decimal128."""
    end = position + 16
    return Decimal128.from_bid(data[position:end]), end


# Each decoder function's signature is:
#   - data: bytes
#   - view: memoryview that references `data`
#   - position: int, beginning of object in 'data' to decode
#   - obj_end: int, end of object to decode in 'data' if variable-length type
#   - opts: a CodecOptions
_ELEMENT_GETTER = {
    _maybe_ord(BSONNUM): _get_float,
    _maybe_ord(BSONSTR): _get_string,
    _maybe_ord(BSONOBJ): _get_object,
    _maybe_ord(BSONARR): _get_array,
    _maybe_ord(BSONBIN): _get_binary,
    _maybe_ord(BSONUND): lambda u, v, w, x, y, z: (None, w),  # Deprecated undefined
    _maybe_ord(BSONOID): _get_oid,
    _maybe_ord(BSONBOO): _get_boolean,
    _maybe_ord(BSONDAT): _get_date,
    _maybe_ord(BSONNUL): lambda u, v, w, x, y, z: (None, w),
    _maybe_ord(BSONRGX): _get_regex,
    _maybe_ord(BSONREF): _get_ref,  # Deprecated DBPointer
    _maybe_ord(BSONCOD): _get_code,
    _maybe_ord(BSONSYM): _get_string,  # Deprecated symbol
    _maybe_ord(BSONCWS): _get_code_w_scope,
    _maybe_ord(BSONINT): _get_int,
    _maybe_ord(BSONTIM): _get_timestamp,
    _maybe_ord(BSONLON): _get_int64,
    _maybe_ord(BSONDEC): _get_decimal128,
    _maybe_ord(BSONMIN): lambda u, v, w, x, y, z: (MinKey(), w),
    _maybe_ord(BSONMAX): lambda u, v, w, x, y, z: (MaxKey(), w)}


if _USE_C:
    def _element_to_dict(data, view, position, obj_end, opts):
        return _cbson._element_to_dict(data, position, obj_end, opts)
else:
    def _element_to_dict(data, view, position, obj_end, opts):
        """Decode a single key, value pair."""
        element_type = data[position]
        position += 1
        element_name, position = _get_c_string(data, view, position, opts)
        try:
            value, position = _ELEMENT_GETTER[element_type](data, view, position,
                                                            obj_end, opts,
                                                            element_name)
        except KeyError:
            _raise_unknown_type(element_type, element_name)

        if opts.type_registry._decoder_map:
            custom_decoder = opts.type_registry._decoder_map.get(type(value))
            if custom_decoder is not None:
                value = custom_decoder(value)

        return element_name, value, position


def _raw_to_dict(data, position, obj_end, opts, result):
    data, view = get_data_and_view(data)
    return _elements_to_dict(data, view, position, obj_end, opts, result)


def _elements_to_dict(data, view, position, obj_end, opts, result=None):
    """Decode a BSON document into result."""
    if result is None:
        result = opts.document_class()
    end = obj_end - 1
    while position < end:
        key, value, position = _element_to_dict(data, view, position, obj_end, opts)
        result[key] = value
    if position != obj_end:
        raise InvalidBSON('bad object or element length')
    return result


def _bson_to_dict(data, opts):
    """Decode a BSON string to document_class."""
    data, view = get_data_and_view(data)
    try:
        if _raw_document_class(opts.document_class):
            return opts.document_class(data, opts)
        _, end = _get_object_size(data, 0, len(data))
        return _elements_to_dict(data, view, 4, end, opts)
    except InvalidBSON:
        raise
    except Exception:
        # Change exception type to InvalidBSON but preserve traceback.
        _, exc_value, exc_tb = sys.exc_info()
        reraise(InvalidBSON, exc_value, exc_tb)
if _USE_C:
    _bson_to_dict = _cbson._bson_to_dict


_PACK_FLOAT = struct.Struct("<d").pack
_PACK_INT = struct.Struct("<i").pack
_PACK_LENGTH_SUBTYPE = struct.Struct("<iB").pack
_PACK_LONG = struct.Struct("<q").pack
_PACK_TIMESTAMP = struct.Struct("<II").pack
_LIST_NAMES = tuple(b(str(i)) + b"\x00" for i in range(1000))


def gen_list_name():
    """Generate "keys" for encoded lists in the sequence
    b"0\x00", b"1\x00", b"2\x00", ...

    The first 1000 keys are returned from a pre-built cache. All
    subsequent keys are generated on the fly.
    """
    for name in _LIST_NAMES:
        yield name

    counter = itertools.count(1000)
    while True:
        yield b(str(next(counter))) + b"\x00"


def _make_c_string_check(string):
    """Make a 'C' string, checking for embedded NUL characters."""
    if isinstance(string, bytes):
        if b"\x00" in string:
            raise InvalidDocument("BSON keys / regex patterns must not "
                                  "contain a NUL character")
        try:
            _utf_8_decode(string, None, True)
            return string + b"\x00"
        except UnicodeError:
            raise InvalidStringData("strings in documents must be valid "
                                    "UTF-8: %r" % string)
    else:
        if "\x00" in string:
            raise InvalidDocument("BSON keys / regex patterns must not "
                                  "contain a NUL character")
        return _utf_8_encode(string)[0] + b"\x00"


def _make_c_string(string):
    """Make a 'C' string."""
    if isinstance(string, bytes):
        try:
            _utf_8_decode(string, None, True)
            return string + b"\x00"
        except UnicodeError:
            raise InvalidStringData("strings in documents must be valid "
                                    "UTF-8: %r" % string)
    else:
        return _utf_8_encode(string)[0] + b"\x00"


if PY3:
    def _make_name(string):
        """Make a 'C' string suitable for a BSON key."""
        # Keys can only be text in python 3.
        if "\x00" in string:
            raise InvalidDocument("BSON keys / regex patterns must not "
                                  "contain a NUL character")
        return _utf_8_encode(string)[0] + b"\x00"
else:
    # Keys can be unicode or bytes in python 2.
    _make_name = _make_c_string_check


def _encode_float(name, value, dummy0, dummy1):
    """Encode a float."""
    return b"\x01" + name + _PACK_FLOAT(value)


if PY3:
    def _encode_bytes(name, value, dummy0, dummy1):
        """Encode a python bytes."""
        # Python3 special case. Store 'bytes' as BSON binary subtype 0.
        return b"\x05" + name + _PACK_INT(len(value)) + b"\x00" + value
else:
    def _encode_bytes(name, value, dummy0, dummy1):
        """Encode a python str (python 2.x)."""
        try:
            _utf_8_decode(value, None, True)
        except UnicodeError:
            raise InvalidStringData("strings in documents must be valid "
                                    "UTF-8: %r" % (value,))
        return b"\x02" + name + _PACK_INT(len(value) + 1) + value + b"\x00"


def _encode_mapping(name, value, check_keys, opts):
    """Encode a mapping type."""
    if _raw_document_class(value):
        return b'\x03' + name + value.raw
    data = b"".join([_element_to_bson(key, val, check_keys, opts)
                     for key, val in iteritems(value)])
    return b"\x03" + name + _PACK_INT(len(data) + 5) + data + b"\x00"


def _encode_dbref(name, value, check_keys, opts):
    """Encode bson.dbref.DBRef."""
    buf = bytearray(b"\x03" + name + b"\x00\x00\x00\x00")
    begin = len(buf) - 4

    buf += _name_value_to_bson(b"$ref\x00",
                               value.collection, check_keys, opts)
    buf += _name_value_to_bson(b"$id\x00",
                               value.id, check_keys, opts)
    if value.database is not None:
        buf += _name_value_to_bson(
            b"$db\x00", value.database, check_keys, opts)
    for key, val in iteritems(value._DBRef__kwargs):
        buf += _element_to_bson(key, val, check_keys, opts)

    buf += b"\x00"
    buf[begin:begin + 4] = _PACK_INT(len(buf) - begin)
    return bytes(buf)


def _encode_list(name, value, check_keys, opts):
    """Encode a list/tuple."""
    lname = gen_list_name()
    data = b"".join([_name_value_to_bson(next(lname), item,
                                         check_keys, opts)
                     for item in value])
    return b"\x04" + name + _PACK_INT(len(data) + 5) + data + b"\x00"


def _encode_text(name, value, dummy0, dummy1):
    """Encode a python unicode (python 2.x) / str (python 3.x)."""
    value = _utf_8_encode(value)[0]
    return b"\x02" + name + _PACK_INT(len(value) + 1) + value + b"\x00"


def _encode_binary(name, value, dummy0, dummy1):
    """Encode bson.binary.Binary."""
    subtype = value.subtype
    if subtype == 2:
        value = _PACK_INT(len(value)) + value
    return b"\x05" + name + _PACK_LENGTH_SUBTYPE(len(value), subtype) + value


def _encode_uuid(name, value, dummy, opts):
    """Encode uuid.UUID."""
    uuid_representation = opts.uuid_representation
    # Python Legacy Common Case
    if uuid_representation == OLD_UUID_SUBTYPE:
        return b"\x05" + name + b'\x10\x00\x00\x00\x03' + value.bytes
    # Java Legacy
    elif uuid_representation == JAVA_LEGACY:
        from_uuid = value.bytes
        data = from_uuid[0:8][::-1] + from_uuid[8:16][::-1]
        return b"\x05" + name + b'\x10\x00\x00\x00\x03' + data
    # C# legacy
    elif uuid_representation == CSHARP_LEGACY:
        # Microsoft GUID representation.
        return b"\x05" + name + b'\x10\x00\x00\x00\x03' + value.bytes_le
    # New
    return b"\x05" + name + b'\x10\x00\x00\x00\x04' + value.bytes


def _encode_objectid(name, value, dummy0, dummy1):
    """Encode bson.objectid.ObjectId."""
    return b"\x07" + name + value.binary


def _encode_bool(name, value, dummy0, dummy1):
    """Encode a python boolean (True/False)."""
    return b"\x08" + name + (value and b"\x01" or b"\x00")


def _encode_datetime(name, value, dummy0, dummy1):
    """Encode datetime.datetime."""
    millis = _datetime_to_millis(value)
    return b"\x09" + name + _PACK_LONG(millis)


def _encode_none(name, dummy0, dummy1, dummy2):
    """Encode python None."""
    return b"\x0A" + name


def _encode_regex(name, value, dummy0, dummy1):
    """Encode a python regex or bson.regex.Regex."""
    flags = value.flags
    # Python 2 common case
    if flags == 0:
        return b"\x0B" + name + _make_c_string_check(value.pattern) + b"\x00"
    # Python 3 common case
    elif flags == re.UNICODE:
        return b"\x0B" + name + _make_c_string_check(value.pattern) + b"u\x00"
    else:
        sflags = b""
        if flags & re.IGNORECASE:
            sflags += b"i"
        if flags & re.LOCALE:
            sflags += b"l"
        if flags & re.MULTILINE:
            sflags += b"m"
        if flags & re.DOTALL:
            sflags += b"s"
        if flags & re.UNICODE:
            sflags += b"u"
        if flags & re.VERBOSE:
            sflags += b"x"
        sflags += b"\x00"
        return b"\x0B" + name + _make_c_string_check(value.pattern) + sflags


def _encode_code(name, value, dummy, opts):
    """Encode bson.code.Code."""
    cstring = _make_c_string(value)
    cstrlen = len(cstring)
    if value.scope is None:
        return b"\x0D" + name + _PACK_INT(cstrlen) + cstring
    scope = _dict_to_bson(value.scope, False, opts, False)
    full_length = _PACK_INT(8 + cstrlen + len(scope))
    return b"\x0F" + name + full_length + _PACK_INT(cstrlen) + cstring + scope


def _encode_int(name, value, dummy0, dummy1):
    """Encode a python int."""
    if -2147483648 <= value <= 2147483647:
        return b"\x10" + name + _PACK_INT(value)
    else:
        try:
            return b"\x12" + name + _PACK_LONG(value)
        except struct.error:
            raise OverflowError("BSON can only handle up to 8-byte ints")


def _encode_timestamp(name, value, dummy0, dummy1):
    """Encode bson.timestamp.Timestamp."""
    return b"\x11" + name + _PACK_TIMESTAMP(value.inc, value.time)


def _encode_long(name, value, dummy0, dummy1):
    """Encode a python long (python 2.x)"""
    try:
        return b"\x12" + name + _PACK_LONG(value)
    except struct.error:
        raise OverflowError("BSON can only handle up to 8-byte ints")


def _encode_decimal128(name, value, dummy0, dummy1):
    """Encode bson.decimal128.Decimal128."""
    return b"\x13" + name + value.bid


def _encode_minkey(name, dummy0, dummy1, dummy2):
    """Encode bson.min_key.MinKey."""
    return b"\xFF" + name


def _encode_maxkey(name, dummy0, dummy1, dummy2):
    """Encode bson.max_key.MaxKey."""
    return b"\x7F" + name


# Each encoder function's signature is:
#   - name: utf-8 bytes
#   - value: a Python data type, e.g. a Python int for _encode_int
#   - check_keys: bool, whether to check for invalid names
#   - opts: a CodecOptions
_ENCODERS = {
    bool: _encode_bool,
    bytes: _encode_bytes,
    datetime.datetime: _encode_datetime,
    dict: _encode_mapping,
    float: _encode_float,
    int: _encode_int,
    list: _encode_list,
    # unicode in py2, str in py3
    text_type: _encode_text,
    tuple: _encode_list,
    type(None): _encode_none,
    uuid.UUID: _encode_uuid,
    Binary: _encode_binary,
    Int64: _encode_long,
    Code: _encode_code,
    DBRef: _encode_dbref,
    MaxKey: _encode_maxkey,
    MinKey: _encode_minkey,
    ObjectId: _encode_objectid,
    Regex: _encode_regex,
    RE_TYPE: _encode_regex,
    SON: _encode_mapping,
    Timestamp: _encode_timestamp,
    UUIDLegacy: _encode_binary,
    Decimal128: _encode_decimal128,
    # Special case. This will never be looked up directly.
    abc.Mapping: _encode_mapping,
}


_MARKERS = {
    5: _encode_binary,
    7: _encode_objectid,
    11: _encode_regex,
    13: _encode_code,
    17: _encode_timestamp,
    18: _encode_long,
    100: _encode_dbref,
    127: _encode_maxkey,
    255: _encode_minkey,
}

if not PY3:
    _ENCODERS[long] = _encode_long


_BUILT_IN_TYPES = tuple(t for t in _ENCODERS)


def _name_value_to_bson(name, value, check_keys, opts,
                        in_custom_call=False,
                        in_fallback_call=False):
    """Encode a single name, value pair."""
    # First see if the type is already cached. KeyError will only ever
    # happen once per subtype.
    try:
        return _ENCODERS[type(value)](name, value, check_keys, opts)
    except KeyError:
        pass

    # Second, fall back to trying _type_marker. This has to be done
    # before the loop below since users could subclass one of our
    # custom types that subclasses a python built-in (e.g. Binary)
    marker = getattr(value, "_type_marker", None)
    if isinstance(marker, int) and marker in _MARKERS:
        func = _MARKERS[marker]
        # Cache this type for faster subsequent lookup.
        _ENCODERS[type(value)] = func
        return func(name, value, check_keys, opts)

    # Third, check if a type encoder is registered for this type.
    # Note that subtypes of registered custom types are not auto-encoded.
    if not in_custom_call and opts.type_registry._encoder_map:
        custom_encoder = opts.type_registry._encoder_map.get(type(value))
        if custom_encoder is not None:
            return _name_value_to_bson(
                name, custom_encoder(value), check_keys, opts,
                in_custom_call=True)

    # Fourth, test each base type. This will only happen once for
    # a subtype of a supported base type. Unlike in the C-extensions, this
    # is done after trying the custom type encoder because checking for each
    # subtype is expensive.
    for base in _BUILT_IN_TYPES:
        if isinstance(value, base):
            func = _ENCODERS[base]
            # Cache this type for faster subsequent lookup.
            _ENCODERS[type(value)] = func
            return func(name, value, check_keys, opts)

    # As a last resort, try using the fallback encoder, if the user has
    # provided one.
    fallback_encoder = opts.type_registry._fallback_encoder
    if not in_fallback_call and fallback_encoder is not None:
        return _name_value_to_bson(
            name, fallback_encoder(value), check_keys, opts,
            in_fallback_call=True)

    raise InvalidDocument(
        "cannot encode object: %r, of type: %r" % (value, type(value)))


def _element_to_bson(key, value, check_keys, opts):
    """Encode a single key, value pair."""
    if not isinstance(key, string_type):
        raise InvalidDocument("documents must have only string keys, "
                              "key was %r" % (key,))
    if check_keys:
        if key.startswith("$"):
            raise InvalidDocument("key %r must not start with '$'" % (key,))
        if "." in key:
            raise InvalidDocument("key %r must not contain '.'" % (key,))

    name = _make_name(key)
    return _name_value_to_bson(name, value, check_keys, opts)


def _dict_to_bson(doc, check_keys, opts, top_level=True):
    """Encode a document to BSON."""
    if _raw_document_class(doc):
        return doc.raw
    try:
        elements = []
        if top_level and "_id" in doc:
            elements.append(_name_value_to_bson(b"_id\x00", doc["_id"],
                                                check_keys, opts))
        for (key, value) in iteritems(doc):
            if not top_level or key != "_id":
                elements.append(_element_to_bson(key, value,
                                                 check_keys, opts))
    except AttributeError:
        raise TypeError("encoder expected a mapping type but got: %r" % (doc,))

    encoded = b"".join(elements)
    return _PACK_INT(len(encoded) + 5) + encoded + b"\x00"
if _USE_C:
    _dict_to_bson = _cbson._dict_to_bson


def _millis_to_datetime(millis, opts):
    """Convert milliseconds since epoch UTC to datetime."""
    diff = ((millis % 1000) + 1000) % 1000
    seconds = (millis - diff) // 1000
    micros = diff * 1000
    if opts.tz_aware:
        dt = EPOCH_AWARE + datetime.timedelta(seconds=seconds,
                                              microseconds=micros)
        if opts.tzinfo:
            dt = dt.astimezone(opts.tzinfo)
        return dt
    else:
        return EPOCH_NAIVE + datetime.timedelta(seconds=seconds,
                                                microseconds=micros)


def _datetime_to_millis(dtm):
    """Convert datetime to milliseconds since epoch UTC."""
    if dtm.utcoffset() is not None:
        dtm = dtm - dtm.utcoffset()
    return int(calendar.timegm(dtm.timetuple()) * 1000 +
               dtm.microsecond // 1000)


_CODEC_OPTIONS_TYPE_ERROR = TypeError(
    "codec_options must be an instance of CodecOptions")


def encode(document, check_keys=False, codec_options=DEFAULT_CODEC_OPTIONS):
    """Encode a document to BSON.

    A document can be any mapping type (like :class:`dict`).

    Raises :class:`TypeError` if `document` is not a mapping type,
    or contains keys that are not instances of
    :class:`basestring` (:class:`str` in python 3). Raises
    :class:`~bson.errors.InvalidDocument` if `document` cannot be
    converted to :class:`BSON`.

    :Parameters:
      - `document`: mapping type representing a document
      - `check_keys` (optional): check if keys start with '$' or
        contain '.', raising :class:`~bson.errors.InvalidDocument` in
        either case
      - `codec_options` (optional): An instance of
        :class:`~bson.codec_options.CodecOptions`.

    .. versionadded:: 3.9
    """
    if not isinstance(codec_options, CodecOptions):
        raise _CODEC_OPTIONS_TYPE_ERROR

    return _dict_to_bson(document, check_keys, codec_options)


def decode(data, codec_options=DEFAULT_CODEC_OPTIONS):
    """Decode BSON to a document.

    By default, returns a BSON document represented as a Python
    :class:`dict`. To use a different :class:`MutableMapping` class,
    configure a :class:`~bson.codec_options.CodecOptions`::

        >>> import collections  # From Python standard library.
        >>> import bson
        >>> from bson.codec_options import CodecOptions
        >>> data = bson.encode({'a': 1})
        >>> decoded_doc = bson.decode(data)
        <type 'dict'>
        >>> options = CodecOptions(document_class=collections.OrderedDict)
        >>> decoded_doc = bson.decode(data, codec_options=options)
        >>> type(decoded_doc)
        <class 'collections.OrderedDict'>

    :Parameters:
      - `data`: the BSON to decode. Any bytes-like object that implements
        the buffer protocol.
      - `codec_options` (optional): An instance of
        :class:`~bson.codec_options.CodecOptions`.

    .. versionadded:: 3.9
    """
    if not isinstance(codec_options, CodecOptions):
        raise _CODEC_OPTIONS_TYPE_ERROR

    return _bson_to_dict(data, codec_options)


def decode_all(data, codec_options=DEFAULT_CODEC_OPTIONS):
    """Decode BSON data to multiple documents.

    `data` must be a bytes-like object implementing the buffer protocol that
    provides concatenated, valid, BSON-encoded documents.

    :Parameters:
      - `data`: BSON data
      - `codec_options` (optional): An instance of
        :class:`~bson.codec_options.CodecOptions`.

    .. versionchanged:: 3.9
       Supports bytes-like objects that implement the buffer protocol.

    .. versionchanged:: 3.0
       Removed `compile_re` option: PyMongo now always represents BSON regular
       expressions as :class:`~bson.regex.Regex` objects. Use
       :meth:`~bson.regex.Regex.try_compile` to attempt to convert from a
       BSON regular expression to a Python regular expression object.

       Replaced `as_class`, `tz_aware`, and `uuid_subtype` options with
       `codec_options`.

    .. versionchanged:: 2.7
       Added `compile_re` option. If set to False, PyMongo represented BSON
       regular expressions as :class:`~bson.regex.Regex` objects instead of
       attempting to compile BSON regular expressions as Python native
       regular expressions, thus preventing errors for some incompatible
       patterns, see `PYTHON-500`_.

    .. _PYTHON-500: https://jira.mongodb.org/browse/PYTHON-500
    """
    data, view = get_data_and_view(data)
    if not isinstance(codec_options, CodecOptions):
        raise _CODEC_OPTIONS_TYPE_ERROR

    data_len = len(data)
    docs = []
    position = 0
    end = data_len - 1
    use_raw = _raw_document_class(codec_options.document_class)
    try:
        while position < end:
            obj_size = _UNPACK_INT_FROM(data, position)[0]
            if data_len - position < obj_size:
                raise InvalidBSON("invalid object size")
            obj_end = position + obj_size - 1
            if data[obj_end] != _OBJEND:
                raise InvalidBSON("bad eoo")
            if use_raw:
                docs.append(
                    codec_options.document_class(
                        data[position:obj_end + 1], codec_options))
            else:
                docs.append(_elements_to_dict(data,
                                              view,
                                              position + 4,
                                              obj_end,
                                              codec_options))
            position += obj_size
        return docs
    except InvalidBSON:
        raise
    except Exception:
        # Change exception type to InvalidBSON but preserve traceback.
        _, exc_value, exc_tb = sys.exc_info()
        reraise(InvalidBSON, exc_value, exc_tb)


if _USE_C:
    decode_all = _cbson.decode_all


def _decode_selective(rawdoc, fields, codec_options):
    if _raw_document_class(codec_options.document_class):
        # If document_class is RawBSONDocument, use vanilla dictionary for
        # decoding command response.
        doc = {}
    else:
        # Else, use the specified document_class.
        doc = codec_options.document_class()
    for key, value in iteritems(rawdoc):
        if key in fields:
            if fields[key] == 1:
                doc[key] = _bson_to_dict(rawdoc.raw, codec_options)[key]
            else:
                doc[key] = _decode_selective(value, fields[key], codec_options)
        else:
            doc[key] = value
    return doc


def _decode_all_selective(data, codec_options, fields):
    """Decode BSON data to a single document while using user-provided
    custom decoding logic.

    `data` must be a string representing a valid, BSON-encoded document.

    :Parameters:
      - `data`: BSON data
      - `codec_options`: An instance of
        :class:`~bson.codec_options.CodecOptions` with user-specified type
        decoders. If no decoders are found, this method is the same as
        ``decode_all``.
      - `fields`: Map of document namespaces where data that needs
        to be custom decoded lives or None. For example, to custom decode a
        list of objects in 'field1.subfield1', the specified value should be
        ``{'field1': {'subfield1': 1}}``. If ``fields``  is an empty map or
        None, this method is the same as ``decode_all``.

    :Returns:
      - `document_list`: Single-member list containing the decoded document.

    .. versionadded:: 3.8
    """
    if not codec_options.type_registry._decoder_map:
        return decode_all(data, codec_options)

    if not fields:
        return decode_all(data, codec_options.with_options(type_registry=None))

    # Decode documents for internal use.
    from bson.raw_bson import RawBSONDocument
    internal_codec_options = codec_options.with_options(
        document_class=RawBSONDocument, type_registry=None)
    _doc = _bson_to_dict(data, internal_codec_options)
    return [_decode_selective(_doc, fields, codec_options,)]


def decode_iter(data, codec_options=DEFAULT_CODEC_OPTIONS):
    """Decode BSON data to multiple documents as a generator.

    Works similarly to the decode_all function, but yields one document at a
    time.

    `data` must be a string of concatenated, valid, BSON-encoded
    documents.

    :Parameters:
      - `data`: BSON data
      - `codec_options` (optional): An instance of
        :class:`~bson.codec_options.CodecOptions`.

    .. versionchanged:: 3.0
       Replaced `as_class`, `tz_aware`, and `uuid_subtype` options with
       `codec_options`.

    .. versionadded:: 2.8
    """
    if not isinstance(codec_options, CodecOptions):
        raise _CODEC_OPTIONS_TYPE_ERROR

    position = 0
    end = len(data) - 1
    while position < end:
        obj_size = _UNPACK_INT_FROM(data, position)[0]
        elements = data[position:position + obj_size]
        position += obj_size

        yield _bson_to_dict(elements, codec_options)


def decode_file_iter(file_obj, codec_options=DEFAULT_CODEC_OPTIONS):
    """Decode bson data from a file to multiple documents as a generator.

    Works similarly to the decode_all function, but reads from the file object
    in chunks and parses bson in chunks, yielding one document at a time.

    :Parameters:
      - `file_obj`: A file object containing BSON data.
      - `codec_options` (optional): An instance of
        :class:`~bson.codec_options.CodecOptions`.

    .. versionchanged:: 3.0
       Replaced `as_class`, `tz_aware`, and `uuid_subtype` options with
       `codec_options`.

    .. versionadded:: 2.8
    """
    while True:
        # Read size of next object.
        size_data = file_obj.read(4)
        if not size_data:
            break  # Finished with file normaly.
        elif len(size_data) != 4:
            raise InvalidBSON("cut off in middle of objsize")
        obj_size = _UNPACK_INT_FROM(size_data, 0)[0] - 4
        elements = size_data + file_obj.read(obj_size)
        yield _bson_to_dict(elements, codec_options)


def is_valid(bson):
    """Check that the given string represents valid :class:`BSON` data.

    Raises :class:`TypeError` if `bson` is not an instance of
    :class:`str` (:class:`bytes` in python 3). Returns ``True``
    if `bson` is valid :class:`BSON`, ``False`` otherwise.

    :Parameters:
      - `bson`: the data to be validated
    """
    if not isinstance(bson, bytes):
        raise TypeError("BSON data must be an instance of a subclass of bytes")

    try:
        _bson_to_dict(bson, DEFAULT_CODEC_OPTIONS)
        return True
    except Exception:
        return False


class BSON(bytes):
    """BSON (Binary JSON) data.

    .. warning:: Using this class to encode and decode BSON adds a performance
       cost. For better performance use the module level functions
       :func:`encode` and :func:`decode` instead.
    """

    @classmethod
    def encode(cls, document, check_keys=False,
               codec_options=DEFAULT_CODEC_OPTIONS):
        """Encode a document to a new :class:`BSON` instance.

        A document can be any mapping type (like :class:`dict`).

        Raises :class:`TypeError` if `document` is not a mapping type,
        or contains keys that are not instances of
        :class:`basestring` (:class:`str` in python 3). Raises
        :class:`~bson.errors.InvalidDocument` if `document` cannot be
        converted to :class:`BSON`.

        :Parameters:
          - `document`: mapping type representing a document
          - `check_keys` (optional): check if keys start with '$' or
            contain '.', raising :class:`~bson.errors.InvalidDocument` in
            either case
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`.

        .. versionchanged:: 3.0
           Replaced `uuid_subtype` option with `codec_options`.
        """
        return cls(encode(document, check_keys, codec_options))

    def decode(self, codec_options=DEFAULT_CODEC_OPTIONS):
        """Decode this BSON data.

        By default, returns a BSON document represented as a Python
        :class:`dict`. To use a different :class:`MutableMapping` class,
        configure a :class:`~bson.codec_options.CodecOptions`::

            >>> import collections  # From Python standard library.
            >>> import bson
            >>> from bson.codec_options import CodecOptions
            >>> data = bson.BSON.encode({'a': 1})
            >>> decoded_doc = bson.BSON(data).decode()
            <type 'dict'>
            >>> options = CodecOptions(document_class=collections.OrderedDict)
            >>> decoded_doc = bson.BSON(data).decode(codec_options=options)
            >>> type(decoded_doc)
            <class 'collections.OrderedDict'>

        :Parameters:
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`.

        .. versionchanged:: 3.0
           Removed `compile_re` option: PyMongo now always represents BSON
           regular expressions as :class:`~bson.regex.Regex` objects. Use
           :meth:`~bson.regex.Regex.try_compile` to attempt to convert from a
           BSON regular expression to a Python regular expression object.

           Replaced `as_class`, `tz_aware`, and `uuid_subtype` options with
           `codec_options`.

        .. versionchanged:: 2.7
           Added `compile_re` option. If set to False, PyMongo represented BSON
           regular expressions as :class:`~bson.regex.Regex` objects instead of
           attempting to compile BSON regular expressions as Python native
           regular expressions, thus preventing errors for some incompatible
           patterns, see `PYTHON-500`_.

        .. _PYTHON-500: https://jira.mongodb.org/browse/PYTHON-500
        """
        return decode(self, codec_options)


def has_c():
    """Is the C extension installed?
    """
    return _USE_C
