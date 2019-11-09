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

"""Tools for specifying BSON codec options."""

import datetime

from abc import abstractmethod
from collections import namedtuple

from bson.py3compat import ABC, abc, abstractproperty, string_type

from bson.binary import (ALL_UUID_REPRESENTATIONS,
                         PYTHON_LEGACY,
                         UUID_REPRESENTATION_NAMES)


_RAW_BSON_DOCUMENT_MARKER = 101


def _raw_document_class(document_class):
    """Determine if a document_class is a RawBSONDocument class."""
    marker = getattr(document_class, '_type_marker', None)
    return marker == _RAW_BSON_DOCUMENT_MARKER


class TypeEncoder(ABC):
    """Base class for defining type codec classes which describe how a
    custom type can be transformed to one of the types BSON understands.

    Codec classes must implement the ``python_type`` attribute, and the
    ``transform_python`` method to support encoding.

    See :ref:`custom-type-type-codec` documentation for an example.
    """
    @abstractproperty
    def python_type(self):
        """The Python type to be converted into something serializable."""
        pass

    @abstractmethod
    def transform_python(self, value):
        """Convert the given Python object into something serializable."""
        pass


class TypeDecoder(ABC):
    """Base class for defining type codec classes which describe how a
    BSON type can be transformed to a custom type.

    Codec classes must implement the ``bson_type`` attribute, and the
    ``transform_bson`` method to support decoding.

    See :ref:`custom-type-type-codec` documentation for an example.
    """
    @abstractproperty
    def bson_type(self):
        """The BSON type to be converted into our own type."""
        pass

    @abstractmethod
    def transform_bson(self, value):
        """Convert the given BSON value into our own type."""
        pass


class TypeCodec(TypeEncoder, TypeDecoder):
    """Base class for defining type codec classes which describe how a
    custom type can be transformed to/from one of the types :mod:`bson`
    can already encode/decode.

    Codec classes must implement the ``python_type`` attribute, and the
    ``transform_python`` method to support encoding, as well as the
    ``bson_type`` attribute, and the ``transform_bson`` method to support
    decoding.

    See :ref:`custom-type-type-codec` documentation for an example.
    """
    pass


class TypeRegistry(object):
    """Encapsulates type codecs used in encoding and / or decoding BSON, as
    well as the fallback encoder. Type registries cannot be modified after
    instantiation.

    ``TypeRegistry`` can be initialized with an iterable of type codecs, and
    a callable for the fallback encoder::

      >>> from bson.codec_options import TypeRegistry
      >>> type_registry = TypeRegistry([Codec1, Codec2, Codec3, ...],
      ...                              fallback_encoder)

    See :ref:`custom-type-type-registry` documentation for an example.

    :Parameters:
      - `type_codecs` (optional): iterable of type codec instances. If
        ``type_codecs`` contains multiple codecs that transform a single
        python or BSON type, the transformation specified by the type codec
        occurring last prevails. A TypeError will be raised if one or more
        type codecs modify the encoding behavior of a built-in :mod:`bson`
        type.
      - `fallback_encoder` (optional): callable that accepts a single,
        unencodable python value and transforms it into a type that
        :mod:`bson` can encode. See :ref:`fallback-encoder-callable`
        documentation for an example.
    """
    def __init__(self, type_codecs=None, fallback_encoder=None):
        self.__type_codecs = list(type_codecs or [])
        self._fallback_encoder = fallback_encoder
        self._encoder_map = {}
        self._decoder_map = {}

        if self._fallback_encoder is not None:
            if not callable(fallback_encoder):
                raise TypeError("fallback_encoder %r is not a callable" % (
                    fallback_encoder))

        for codec in self.__type_codecs:
            is_valid_codec = False
            if isinstance(codec, TypeEncoder):
                self._validate_type_encoder(codec)
                is_valid_codec = True
                self._encoder_map[codec.python_type] = codec.transform_python
            if isinstance(codec, TypeDecoder):
                is_valid_codec = True
                self._decoder_map[codec.bson_type] = codec.transform_bson
            if not is_valid_codec:
                raise TypeError(
                    "Expected an instance of %s, %s, or %s, got %r instead" % (
                        TypeEncoder.__name__, TypeDecoder.__name__,
                        TypeCodec.__name__, codec))

    def _validate_type_encoder(self, codec):
        from bson import _BUILT_IN_TYPES
        for pytype in _BUILT_IN_TYPES:
            if issubclass(codec.python_type, pytype):
                err_msg = ("TypeEncoders cannot change how built-in types are "
                           "encoded (encoder %s transforms type %s)" %
                           (codec, pytype))
                raise TypeError(err_msg)

    def __repr__(self):
        return ('%s(type_codecs=%r, fallback_encoder=%r)' % (
            self.__class__.__name__, self.__type_codecs,
            self._fallback_encoder))

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return ((self._decoder_map == other._decoder_map) and
                (self._encoder_map == other._encoder_map) and
                (self._fallback_encoder == other._fallback_encoder))


_options_base = namedtuple(
    'CodecOptions',
    ('document_class', 'tz_aware', 'uuid_representation',
     'unicode_decode_error_handler', 'tzinfo', 'type_registry'))


class CodecOptions(_options_base):
    """Encapsulates options used encoding and / or decoding BSON.

    The `document_class` option is used to define a custom type for use
    decoding BSON documents. Access to the underlying raw BSON bytes for
    a document is available using the :class:`~bson.raw_bson.RawBSONDocument`
    type::

      >>> from bson.raw_bson import RawBSONDocument
      >>> from bson.codec_options import CodecOptions
      >>> codec_options = CodecOptions(document_class=RawBSONDocument)
      >>> coll = db.get_collection('test', codec_options=codec_options)
      >>> doc = coll.find_one()
      >>> doc.raw
      '\\x16\\x00\\x00\\x00\\x07_id\\x00[0\\x165\\x91\\x10\\xea\\x14\\xe8\\xc5\\x8b\\x93\\x00'

    The document class can be any type that inherits from
    :class:`~collections.MutableMapping`::

      >>> class AttributeDict(dict):
      ...     # A dict that supports attribute access.
      ...     def __getattr__(self, key):
      ...         return self[key]
      ...     def __setattr__(self, key, value):
      ...         self[key] = value
      ...
      >>> codec_options = CodecOptions(document_class=AttributeDict)
      >>> coll = db.get_collection('test', codec_options=codec_options)
      >>> doc = coll.find_one()
      >>> doc._id
      ObjectId('5b3016359110ea14e8c58b93')

    See :doc:`/examples/datetimes` for examples using the `tz_aware` and
    `tzinfo` options.

    See :class:`~bson.binary.UUIDLegacy` for examples using the
    `uuid_representation` option.

    :Parameters:
      - `document_class`: BSON documents returned in queries will be decoded
        to an instance of this class. Must be a subclass of
        :class:`~collections.MutableMapping`. Defaults to :class:`dict`.
      - `tz_aware`: If ``True``, BSON datetimes will be decoded to timezone
        aware instances of :class:`~datetime.datetime`. Otherwise they will be
        naive. Defaults to ``False``.
      - `uuid_representation`: The BSON representation to use when encoding
        and decoding instances of :class:`~uuid.UUID`. Defaults to
        :data:`~bson.binary.PYTHON_LEGACY`.
      - `unicode_decode_error_handler`: The error handler to apply when
        a Unicode-related error occurs during BSON decoding that would
        otherwise raise :exc:`UnicodeDecodeError`. Valid options include
        'strict', 'replace', and 'ignore'. Defaults to 'strict'.
      - `tzinfo`: A :class:`~datetime.tzinfo` subclass that specifies the
        timezone to/from which :class:`~datetime.datetime` objects should be
        encoded/decoded.
      - `type_registry`: Instance of :class:`TypeRegistry` used to customize
        encoding and decoding behavior.

    .. versionadded:: 3.8
       `type_registry` attribute.

    .. warning:: Care must be taken when changing
       `unicode_decode_error_handler` from its default value ('strict').
       The 'replace' and 'ignore' modes should not be used when documents
       retrieved from the server will be modified in the client application
       and stored back to the server.
    """

    def __new__(cls, document_class=dict,
                tz_aware=False, uuid_representation=PYTHON_LEGACY,
                unicode_decode_error_handler="strict",
                tzinfo=None, type_registry=None):
        if not (issubclass(document_class, abc.MutableMapping) or
                _raw_document_class(document_class)):
            raise TypeError("document_class must be dict, bson.son.SON, "
                            "bson.raw_bson.RawBSONDocument, or a "
                            "sublass of collections.MutableMapping")
        if not isinstance(tz_aware, bool):
            raise TypeError("tz_aware must be True or False")
        if uuid_representation not in ALL_UUID_REPRESENTATIONS:
            raise ValueError("uuid_representation must be a value "
                             "from bson.binary.ALL_UUID_REPRESENTATIONS")
        if not isinstance(unicode_decode_error_handler, (string_type, None)):
            raise ValueError("unicode_decode_error_handler must be a string "
                             "or None")
        if tzinfo is not None:
            if not isinstance(tzinfo, datetime.tzinfo):
                raise TypeError(
                    "tzinfo must be an instance of datetime.tzinfo")
            if not tz_aware:
                raise ValueError(
                    "cannot specify tzinfo without also setting tz_aware=True")

        type_registry = type_registry or TypeRegistry()

        if not isinstance(type_registry, TypeRegistry):
            raise TypeError("type_registry must be an instance of TypeRegistry")

        return tuple.__new__(
            cls, (document_class, tz_aware, uuid_representation,
                  unicode_decode_error_handler, tzinfo, type_registry))

    def _arguments_repr(self):
        """Representation of the arguments used to create this object."""
        document_class_repr = (
            'dict' if self.document_class is dict
            else repr(self.document_class))

        uuid_rep_repr = UUID_REPRESENTATION_NAMES.get(self.uuid_representation,
                                                      self.uuid_representation)

        return ('document_class=%s, tz_aware=%r, uuid_representation=%s, '
                'unicode_decode_error_handler=%r, tzinfo=%r, '
                'type_registry=%r' %
                (document_class_repr, self.tz_aware, uuid_rep_repr,
                 self.unicode_decode_error_handler, self.tzinfo,
                 self.type_registry))

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self._arguments_repr())

    def with_options(self, **kwargs):
        """Make a copy of this CodecOptions, overriding some options::

            >>> from bson.codec_options import DEFAULT_CODEC_OPTIONS
            >>> DEFAULT_CODEC_OPTIONS.tz_aware
            False
            >>> options = DEFAULT_CODEC_OPTIONS.with_options(tz_aware=True)
            >>> options.tz_aware
            True

        .. versionadded:: 3.5
        """
        return CodecOptions(
            kwargs.get('document_class', self.document_class),
            kwargs.get('tz_aware', self.tz_aware),
            kwargs.get('uuid_representation', self.uuid_representation),
            kwargs.get('unicode_decode_error_handler',
                       self.unicode_decode_error_handler),
            kwargs.get('tzinfo', self.tzinfo),
            kwargs.get('type_registry', self.type_registry)
        )


DEFAULT_CODEC_OPTIONS = CodecOptions()


def _parse_codec_options(options):
    """Parse BSON codec options."""
    return CodecOptions(
        document_class=options.get(
            'document_class', DEFAULT_CODEC_OPTIONS.document_class),
        tz_aware=options.get(
            'tz_aware', DEFAULT_CODEC_OPTIONS.tz_aware),
        uuid_representation=options.get(
            'uuidrepresentation', DEFAULT_CODEC_OPTIONS.uuid_representation),
        unicode_decode_error_handler=options.get(
            'unicode_decode_error_handler',
            DEFAULT_CODEC_OPTIONS.unicode_decode_error_handler),
        tzinfo=options.get('tzinfo', DEFAULT_CODEC_OPTIONS.tzinfo),
        type_registry=options.get(
            'type_registry', DEFAULT_CODEC_OPTIONS.type_registry))
