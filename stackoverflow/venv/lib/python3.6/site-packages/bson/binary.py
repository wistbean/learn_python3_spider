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

from uuid import UUID

from bson.py3compat import PY3

"""Tools for representing BSON binary data.
"""

BINARY_SUBTYPE = 0
"""BSON binary subtype for binary data.

This is the default subtype for binary data.
"""

FUNCTION_SUBTYPE = 1
"""BSON binary subtype for functions.
"""

OLD_BINARY_SUBTYPE = 2
"""Old BSON binary subtype for binary data.

This is the old default subtype, the current
default is :data:`BINARY_SUBTYPE`.
"""

OLD_UUID_SUBTYPE = 3
"""Old BSON binary subtype for a UUID.

:class:`uuid.UUID` instances will automatically be encoded
by :mod:`bson` using this subtype.

.. versionadded:: 2.1
"""

UUID_SUBTYPE = 4
"""BSON binary subtype for a UUID.

This is the new BSON binary subtype for UUIDs. The
current default is :data:`OLD_UUID_SUBTYPE`.

.. versionchanged:: 2.1
   Changed to subtype 4.
"""

STANDARD = UUID_SUBTYPE
"""The standard UUID representation.

:class:`uuid.UUID` instances will automatically be encoded to
and decoded from BSON binary, using RFC-4122 byte order with
binary subtype :data:`UUID_SUBTYPE`.

.. versionadded:: 3.0
"""

PYTHON_LEGACY = OLD_UUID_SUBTYPE
"""The Python legacy UUID representation.

:class:`uuid.UUID` instances will automatically be encoded to
and decoded from BSON binary, using RFC-4122 byte order with
binary subtype :data:`OLD_UUID_SUBTYPE`.

.. versionadded:: 3.0
"""

JAVA_LEGACY = 5
"""The Java legacy UUID representation.

:class:`uuid.UUID` instances will automatically be encoded to
and decoded from BSON binary subtype :data:`OLD_UUID_SUBTYPE`,
using the Java driver's legacy byte order.

.. versionchanged:: 3.6
  BSON binary subtype 4 is decoded using RFC-4122 byte order.
.. versionadded:: 2.3
"""

CSHARP_LEGACY = 6
"""The C#/.net legacy UUID representation.

:class:`uuid.UUID` instances will automatically be encoded to
and decoded from BSON binary subtype :data:`OLD_UUID_SUBTYPE`,
using the C# driver's legacy byte order.

.. versionchanged:: 3.6
  BSON binary subtype 4 is decoded using RFC-4122 byte order.
.. versionadded:: 2.3
"""

ALL_UUID_SUBTYPES = (OLD_UUID_SUBTYPE, UUID_SUBTYPE)
ALL_UUID_REPRESENTATIONS = (STANDARD, PYTHON_LEGACY, JAVA_LEGACY, CSHARP_LEGACY)
UUID_REPRESENTATION_NAMES = {
    PYTHON_LEGACY: 'PYTHON_LEGACY',
    STANDARD: 'STANDARD',
    JAVA_LEGACY: 'JAVA_LEGACY',
    CSHARP_LEGACY: 'CSHARP_LEGACY'}

MD5_SUBTYPE = 5
"""BSON binary subtype for an MD5 hash.
"""

USER_DEFINED_SUBTYPE = 128
"""BSON binary subtype for any user defined structure.
"""


class Binary(bytes):
    """Representation of BSON binary data.

    This is necessary because we want to represent Python strings as
    the BSON string type. We need to wrap binary data so we can tell
    the difference between what should be considered binary data and
    what should be considered a string when we encode to BSON.

    Raises TypeError if `data` is not an instance of :class:`bytes`
    (:class:`str` in python 2) or `subtype` is not an instance of
    :class:`int`. Raises ValueError if `subtype` is not in [0, 256).

    .. note::
      In python 3 instances of Binary with subtype 0 will be decoded
      directly to :class:`bytes`.

    :Parameters:
      - `data`: the binary data to represent. Can be any bytes-like type
        that implements the buffer protocol.
      - `subtype` (optional): the `binary subtype
        <http://bsonspec.org/#/specification>`_
        to use

    .. versionchanged:: 3.9
      Support any bytes-like type that implements the buffer protocol.
    """

    _type_marker = 5

    def __new__(cls, data, subtype=BINARY_SUBTYPE):
        if not isinstance(subtype, int):
            raise TypeError("subtype must be an instance of int")
        if subtype >= 256 or subtype < 0:
            raise ValueError("subtype must be contained in [0, 256)")
        # Support any type that implements the buffer protocol.
        self = bytes.__new__(cls, memoryview(data).tobytes())
        self.__subtype = subtype
        return self

    @property
    def subtype(self):
        """Subtype of this binary data.
        """
        return self.__subtype

    def __getnewargs__(self):
        # Work around http://bugs.python.org/issue7382
        data = super(Binary, self).__getnewargs__()[0]
        if PY3 and not isinstance(data, bytes):
            data = data.encode('latin-1')
        return data, self.__subtype

    def __eq__(self, other):
        if isinstance(other, Binary):
            return ((self.__subtype, bytes(self)) ==
                    (other.subtype, bytes(other)))
        # We don't return NotImplemented here because if we did then
        # Binary("foo") == "foo" would return True, since Binary is a
        # subclass of str...
        return False

    def __hash__(self):
        return super(Binary, self).__hash__() ^ hash(self.__subtype)

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "Binary(%s, %s)" % (bytes.__repr__(self), self.__subtype)


class UUIDLegacy(Binary):
    """UUID wrapper to support working with UUIDs stored as PYTHON_LEGACY.

    .. doctest::

      >>> import uuid
      >>> from bson.binary import Binary, UUIDLegacy, STANDARD
      >>> from bson.codec_options import CodecOptions
      >>> my_uuid = uuid.uuid4()
      >>> coll = db.get_collection('test',
      ...                          CodecOptions(uuid_representation=STANDARD))
      >>> coll.insert_one({'uuid': Binary(my_uuid.bytes, 3)}).inserted_id
      ObjectId('...')
      >>> coll.count_documents({'uuid': my_uuid})
      0
      >>> coll.count_documents({'uuid': UUIDLegacy(my_uuid)})
      1
      >>> coll.find({'uuid': UUIDLegacy(my_uuid)})[0]['uuid']
      UUID('...')
      >>>
      >>> # Convert from subtype 3 to subtype 4
      >>> doc = coll.find_one({'uuid': UUIDLegacy(my_uuid)})
      >>> coll.replace_one({"_id": doc["_id"]}, doc).matched_count
      1
      >>> coll.count_documents({'uuid': UUIDLegacy(my_uuid)})
      0
      >>> coll.count_documents({'uuid': {'$in': [UUIDLegacy(my_uuid), my_uuid]}})
      1
      >>> coll.find_one({'uuid': my_uuid})['uuid']
      UUID('...')

    Raises TypeError if `obj` is not an instance of :class:`~uuid.UUID`.

    :Parameters:
      - `obj`: An instance of :class:`~uuid.UUID`.
    """

    def __new__(cls, obj):
        if not isinstance(obj, UUID):
            raise TypeError("obj must be an instance of uuid.UUID")
        self = Binary.__new__(cls, obj.bytes, OLD_UUID_SUBTYPE)
        self.__uuid = obj
        return self

    def __getnewargs__(self):
        # Support copy and deepcopy
        return (self.__uuid,)

    @property
    def uuid(self):
        """UUID instance wrapped by this UUIDLegacy instance.
        """
        return self.__uuid

    def __repr__(self):
        return "UUIDLegacy('%s')" % self.__uuid
