# Copyright 2011-present MongoDB, Inc.
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


"""Functions and classes common to multiple pymongo modules."""

import datetime
import warnings

from bson import SON
from bson.binary import (STANDARD, PYTHON_LEGACY,
                         JAVA_LEGACY, CSHARP_LEGACY)
from bson.codec_options import CodecOptions, TypeRegistry
from bson.py3compat import abc, integer_types, iteritems, string_type
from bson.raw_bson import RawBSONDocument
from pymongo.auth import MECHANISMS
from pymongo.compression_support import (validate_compressors,
                                         validate_zlib_compression_level)
from pymongo.driver_info import DriverInfo
from pymongo.encryption_options import validate_auto_encryption_opts_or_none
from pymongo.errors import ConfigurationError
from pymongo.monitoring import _validate_event_listeners
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _MONGOS_MODES, _ServerMode
from pymongo.ssl_support import (validate_cert_reqs,
                                 validate_allow_invalid_certs)
from pymongo.write_concern import DEFAULT_WRITE_CONCERN, WriteConcern

try:
    from collections import OrderedDict
    ORDERED_TYPES = (SON, OrderedDict)
except ImportError:
    ORDERED_TYPES = (SON,)


# Defaults until we connect to a server and get updated limits.
MAX_BSON_SIZE = 16 * (1024 ** 2)
MAX_MESSAGE_SIZE = 2 * MAX_BSON_SIZE
MIN_WIRE_VERSION = 0
MAX_WIRE_VERSION = 0
MAX_WRITE_BATCH_SIZE = 1000

# What this version of PyMongo supports.
MIN_SUPPORTED_SERVER_VERSION = "2.6"
MIN_SUPPORTED_WIRE_VERSION = 2
MAX_SUPPORTED_WIRE_VERSION = 8

# Frequency to call ismaster on servers, in seconds.
HEARTBEAT_FREQUENCY = 10

# Frequency to process kill-cursors, in seconds. See MongoClient.close_cursor.
KILL_CURSOR_FREQUENCY = 1

# Frequency to process events queue, in seconds.
EVENTS_QUEUE_FREQUENCY = 1

# How long to wait, in seconds, for a suitable server to be found before
# aborting an operation. For example, if the client attempts an insert
# during a replica set election, SERVER_SELECTION_TIMEOUT governs the
# longest it is willing to wait for a new primary to be found.
SERVER_SELECTION_TIMEOUT = 30

# Spec requires at least 500ms between ismaster calls.
MIN_HEARTBEAT_INTERVAL = 0.5

# Spec requires at least 60s between SRV rescans.
MIN_SRV_RESCAN_INTERVAL = 60

# Default connectTimeout in seconds.
CONNECT_TIMEOUT = 20.0

# Default value for maxPoolSize.
MAX_POOL_SIZE = 100

# Default value for minPoolSize.
MIN_POOL_SIZE = 0

# Default value for maxIdleTimeMS.
MAX_IDLE_TIME_MS = None

# Default value for maxIdleTimeMS in seconds.
MAX_IDLE_TIME_SEC = None

# Default value for waitQueueTimeoutMS in seconds.
WAIT_QUEUE_TIMEOUT = None

# Default value for localThresholdMS.
LOCAL_THRESHOLD_MS = 15

# Default value for retryWrites.
RETRY_WRITES = True

# Default value for retryReads.
RETRY_READS = True

# mongod/s 2.6 and above return code 59 when a command doesn't exist.
COMMAND_NOT_FOUND_CODES = (59,)

# Error codes to ignore if GridFS calls createIndex on a secondary
UNAUTHORIZED_CODES = (13, 16547, 16548)

# Maximum number of sessions to send in a single endSessions command.
# From the driver sessions spec.
_MAX_END_SESSIONS = 10000


def partition_node(node):
    """Split a host:port string into (host, int(port)) pair."""
    host = node
    port = 27017
    idx = node.rfind(':')
    if idx != -1:
        host, port = node[:idx], int(node[idx + 1:])
    if host.startswith('['):
        host = host[1:-1]
    return host, port


def clean_node(node):
    """Split and normalize a node name from an ismaster response."""
    host, port = partition_node(node)

    # Normalize hostname to lowercase, since DNS is case-insensitive:
    # http://tools.ietf.org/html/rfc4343
    # This prevents useless rediscovery if "foo.com" is in the seed list but
    # "FOO.com" is in the ismaster response.
    return host.lower(), port


def raise_config_error(key, dummy):
    """Raise ConfigurationError with the given key name."""
    raise ConfigurationError("Unknown option %s" % (key,))


# Mapping of URI uuid representation options to valid subtypes.
_UUID_REPRESENTATIONS = {
    'standard': STANDARD,
    'pythonLegacy': PYTHON_LEGACY,
    'javaLegacy': JAVA_LEGACY,
    'csharpLegacy': CSHARP_LEGACY
}


def validate_boolean(option, value):
    """Validates that 'value' is True or False."""
    if isinstance(value, bool):
        return value
    raise TypeError("%s must be True or False" % (option,))


def validate_boolean_or_string(option, value):
    """Validates that value is True, False, 'true', or 'false'."""
    if isinstance(value, string_type):
        if value not in ('true', 'false'):
            raise ValueError("The value of %s must be "
                             "'true' or 'false'" % (option,))
        return value == 'true'
    return validate_boolean(option, value)


def validate_integer(option, value):
    """Validates that 'value' is an integer (or basestring representation).
    """
    if isinstance(value, integer_types):
        return value
    elif isinstance(value, string_type):
        try:
            return int(value)
        except ValueError:
            raise ValueError("The value of %s must be "
                             "an integer" % (option,))
    raise TypeError("Wrong type for %s, value must be an integer" % (option,))


def validate_positive_integer(option, value):
    """Validate that 'value' is a positive integer, which does not include 0.
    """
    val = validate_integer(option, value)
    if val <= 0:
        raise ValueError("The value of %s must be "
                         "a positive integer" % (option,))
    return val


def validate_non_negative_integer(option, value):
    """Validate that 'value' is a positive integer or 0.
    """
    val = validate_integer(option, value)
    if val < 0:
        raise ValueError("The value of %s must be "
                         "a non negative integer" % (option,))
    return val


def validate_readable(option, value):
    """Validates that 'value' is file-like and readable.
    """
    if value is None:
        return value
    # First make sure its a string py3.3 open(True, 'r') succeeds
    # Used in ssl cert checking due to poor ssl module error reporting
    value = validate_string(option, value)
    open(value, 'r').close()
    return value


def validate_positive_integer_or_none(option, value):
    """Validate that 'value' is a positive integer or None.
    """
    if value is None:
        return value
    return validate_positive_integer(option, value)


def validate_non_negative_integer_or_none(option, value):
    """Validate that 'value' is a positive integer or 0 or None.
    """
    if value is None:
        return value
    return validate_non_negative_integer(option, value)


def validate_string(option, value):
    """Validates that 'value' is an instance of `basestring` for Python 2
    or `str` for Python 3.
    """
    if isinstance(value, string_type):
        return value
    raise TypeError("Wrong type for %s, value must be "
                    "an instance of %s" % (option, string_type.__name__))


def validate_string_or_none(option, value):
    """Validates that 'value' is an instance of `basestring` or `None`.
    """
    if value is None:
        return value
    return validate_string(option, value)


def validate_int_or_basestring(option, value):
    """Validates that 'value' is an integer or string.
    """
    if isinstance(value, integer_types):
        return value
    elif isinstance(value, string_type):
        try:
            return int(value)
        except ValueError:
            return value
    raise TypeError("Wrong type for %s, value must be an "
                    "integer or a string" % (option,))


def validate_non_negative_int_or_basestring(option, value):
    """Validates that 'value' is an integer or string.
    """
    if isinstance(value, integer_types):
        return value
    elif isinstance(value, string_type):
        try:
            val = int(value)
        except ValueError:
            return value
        return validate_non_negative_integer(option, val)
    raise TypeError("Wrong type for %s, value must be an "
                    "non negative integer or a string" % (option,))


def validate_positive_float(option, value):
    """Validates that 'value' is a float, or can be converted to one, and is
       positive.
    """
    errmsg = "%s must be an integer or float" % (option,)
    try:
        value = float(value)
    except ValueError:
        raise ValueError(errmsg)
    except TypeError:
        raise TypeError(errmsg)

    # float('inf') doesn't work in 2.4 or 2.5 on Windows, so just cap floats at
    # one billion - this is a reasonable approximation for infinity
    if not 0 < value < 1e9:
        raise ValueError("%s must be greater than 0 and "
                         "less than one billion" % (option,))
    return value


def validate_positive_float_or_zero(option, value):
    """Validates that 'value' is 0 or a positive float, or can be converted to
    0 or a positive float.
    """
    if value == 0 or value == "0":
        return 0
    return validate_positive_float(option, value)


def validate_timeout_or_none(option, value):
    """Validates a timeout specified in milliseconds returning
    a value in floating point seconds.
    """
    if value is None:
        return value
    return validate_positive_float(option, value) / 1000.0


def validate_timeout_or_zero(option, value):
    """Validates a timeout specified in milliseconds returning
    a value in floating point seconds for the case where None is an error
    and 0 is valid. Setting the timeout to nothing in the URI string is a
    config error.
    """
    if value is None:
        raise ConfigurationError("%s cannot be None" % (option, ))
    if value == 0 or value == "0":
        return 0
    return validate_positive_float(option, value) / 1000.0


def validate_max_staleness(option, value):
    """Validates maxStalenessSeconds according to the Max Staleness Spec."""
    if value == -1 or value == "-1":
        # Default: No maximum staleness.
        return -1
    return validate_positive_integer(option, value)


def validate_read_preference(dummy, value):
    """Validate a read preference.
    """
    if not isinstance(value, _ServerMode):
        raise TypeError("%r is not a read preference." % (value,))
    return value


def validate_read_preference_mode(dummy, value):
    """Validate read preference mode for a MongoReplicaSetClient.

    .. versionchanged:: 3.5
       Returns the original ``value`` instead of the validated read preference
       mode.
    """
    if value not in _MONGOS_MODES:
        raise ValueError("%s is not a valid read preference" % (value,))
    return value


def validate_auth_mechanism(option, value):
    """Validate the authMechanism URI option.
    """
    # CRAM-MD5 is for server testing only. Undocumented,
    # unsupported, may be removed at any time. You have
    # been warned.
    if value not in MECHANISMS and value != 'CRAM-MD5':
        raise ValueError("%s must be in %s" % (option, tuple(MECHANISMS)))
    return value


def validate_uuid_representation(dummy, value):
    """Validate the uuid representation option selected in the URI.
    """
    try:
        return _UUID_REPRESENTATIONS[value]
    except KeyError:
        raise ValueError("%s is an invalid UUID representation. "
                         "Must be one of "
                         "%s" % (value, tuple(_UUID_REPRESENTATIONS)))


def validate_read_preference_tags(name, value):
    """Parse readPreferenceTags if passed as a client kwarg.
    """
    if not isinstance(value, list):
        value = [value]

    tag_sets = []
    for tag_set in value:
        if tag_set == '':
            tag_sets.append({})
            continue
        try:
            tag_sets.append(dict([tag.split(":")
                                  for tag in tag_set.split(",")]))
        except Exception:
            raise ValueError("%r not a valid "
                             "value for %s" % (tag_set, name))
    return tag_sets


_MECHANISM_PROPS = frozenset(['SERVICE_NAME',
                              'CANONICALIZE_HOST_NAME',
                              'SERVICE_REALM'])


def validate_auth_mechanism_properties(option, value):
    """Validate authMechanismProperties."""
    value = validate_string(option, value)
    props = {}
    for opt in value.split(','):
        try:
            key, val = opt.split(':')
        except ValueError:
            raise ValueError("auth mechanism properties must be "
                             "key:value pairs like SERVICE_NAME:"
                             "mongodb, not %s." % (opt,))
        if key not in _MECHANISM_PROPS:
            raise ValueError("%s is not a supported auth "
                             "mechanism property. Must be one of "
                             "%s." % (key, tuple(_MECHANISM_PROPS)))
        if key == 'CANONICALIZE_HOST_NAME':
            props[key] = validate_boolean_or_string(key, val)
        else:
            props[key] = val

    return props


def validate_document_class(option, value):
    """Validate the document_class option."""
    if not issubclass(value, (abc.MutableMapping, RawBSONDocument)):
        raise TypeError("%s must be dict, bson.son.SON, "
                        "bson.raw_bson.RawBSONDocument, or a "
                        "sublass of collections.MutableMapping" % (option,))
    return value


def validate_type_registry(option, value):
    """Validate the type_registry option."""
    if value is not None and not isinstance(value, TypeRegistry):
        raise TypeError("%s must be an instance of %s" % (
            option, TypeRegistry))
    return value


def validate_list(option, value):
    """Validates that 'value' is a list."""
    if not isinstance(value, list):
        raise TypeError("%s must be a list" % (option,))
    return value


def validate_list_or_none(option, value):
    """Validates that 'value' is a list or None."""
    if value is None:
        return value
    return validate_list(option, value)


def validate_list_or_mapping(option, value):
    """Validates that 'value' is a list or a document."""
    if not isinstance(value, (abc.Mapping, list)):
        raise TypeError("%s must either be a list or an instance of dict, "
                        "bson.son.SON, or any other type that inherits from "
                        "collections.Mapping" % (option,))


def validate_is_mapping(option, value):
    """Validate the type of method arguments that expect a document."""
    if not isinstance(value, abc.Mapping):
        raise TypeError("%s must be an instance of dict, bson.son.SON, or "
                        "any other type that inherits from "
                        "collections.Mapping" % (option,))


def validate_is_document_type(option, value):
    """Validate the type of method arguments that expect a MongoDB document."""
    if not isinstance(value, (abc.MutableMapping, RawBSONDocument)):
        raise TypeError("%s must be an instance of dict, bson.son.SON, "
                        "bson.raw_bson.RawBSONDocument, or "
                        "a type that inherits from "
                        "collections.MutableMapping" % (option,))


def validate_appname_or_none(option, value):
    """Validate the appname option."""
    if value is None:
        return value
    validate_string(option, value)
    # We need length in bytes, so encode utf8 first.
    if len(value.encode('utf-8')) > 128:
        raise ValueError("%s must be <= 128 bytes" % (option,))
    return value


def validate_driver_or_none(option, value):
    """Validate the driver keyword arg."""
    if value is None:
        return value
    if not isinstance(value, DriverInfo):
        raise TypeError("%s must be an instance of DriverInfo" % (option,))
    return value


def validate_is_callable_or_none(option, value):
    """Validates that 'value' is a callable."""
    if value is None:
        return value
    if not callable(value):
        raise ValueError("%s must be a callable" % (option,))
    return value


def validate_ok_for_replace(replacement):
    """Validate a replacement document."""
    validate_is_mapping("replacement", replacement)
    # Replacement can be {}
    if replacement and not isinstance(replacement, RawBSONDocument):
        first = next(iter(replacement))
        if first.startswith('$'):
            raise ValueError('replacement can not include $ operators')


def validate_ok_for_update(update):
    """Validate an update document."""
    validate_list_or_mapping("update", update)
    # Update cannot be {}.
    if not update:
        raise ValueError('update cannot be empty')

    is_document = not isinstance(update, list)
    first = next(iter(update))
    if is_document and not first.startswith('$'):
        raise ValueError('update only works with $ operators')


_UNICODE_DECODE_ERROR_HANDLERS = frozenset(['strict', 'replace', 'ignore'])


def validate_unicode_decode_error_handler(dummy, value):
    """Validate the Unicode decode error handler option of CodecOptions.
    """
    if value not in _UNICODE_DECODE_ERROR_HANDLERS:
        raise ValueError("%s is an invalid Unicode decode error handler. "
                         "Must be one of "
                         "%s" % (value, tuple(_UNICODE_DECODE_ERROR_HANDLERS)))
    return value


def validate_tzinfo(dummy, value):
    """Validate the tzinfo option
    """
    if value is not None and not isinstance(value, datetime.tzinfo):
        raise TypeError("%s must be an instance of datetime.tzinfo" % value)
    return value


# Dictionary where keys are the names of public URI options, and values
# are lists of aliases for that option. Aliases of option names are assumed
# to have been deprecated.
URI_OPTIONS_ALIAS_MAP = {
    'journal': ['j'],
    'wtimeoutms': ['wtimeout'],
    'tls': ['ssl'],
    'tlsallowinvalidcertificates': ['ssl_cert_reqs'],
    'tlsallowinvalidhostnames': ['ssl_match_hostname'],
    'tlscrlfile': ['ssl_crlfile'],
    'tlscafile': ['ssl_ca_certs'],
    'tlscertificatekeyfile': ['ssl_certfile'],
    'tlscertificatekeyfilepassword': ['ssl_pem_passphrase'],
}

# Dictionary where keys are the names of URI options, and values
# are functions that validate user-input values for that option. If an option
# alias uses a different validator than its public counterpart, it should be
# included here as a key, value pair.
URI_OPTIONS_VALIDATOR_MAP = {
    'appname': validate_appname_or_none,
    'authmechanism': validate_auth_mechanism,
    'authmechanismproperties': validate_auth_mechanism_properties,
    'authsource': validate_string,
    'compressors': validate_compressors,
    'connecttimeoutms': validate_timeout_or_none,
    'heartbeatfrequencyms': validate_timeout_or_none,
    'journal': validate_boolean_or_string,
    'localthresholdms': validate_positive_float_or_zero,
    'maxidletimems': validate_timeout_or_none,
    'maxpoolsize': validate_positive_integer_or_none,
    'maxstalenessseconds': validate_max_staleness,
    'readconcernlevel': validate_string_or_none,
    'readpreference': validate_read_preference_mode,
    'readpreferencetags': validate_read_preference_tags,
    'replicaset': validate_string_or_none,
    'retryreads': validate_boolean_or_string,
    'retrywrites': validate_boolean_or_string,
    'serverselectiontimeoutms': validate_timeout_or_zero,
    'sockettimeoutms': validate_timeout_or_none,
    'ssl_keyfile': validate_readable,
    'tls': validate_boolean_or_string,
    'tlsallowinvalidcertificates': validate_allow_invalid_certs,
    'ssl_cert_reqs': validate_cert_reqs,
    'tlsallowinvalidhostnames': lambda *x: not validate_boolean_or_string(*x),
    'ssl_match_hostname': validate_boolean_or_string,
    'tlscafile': validate_readable,
    'tlscertificatekeyfile': validate_readable,
    'tlscertificatekeyfilepassword': validate_string_or_none,
    'tlsinsecure': validate_boolean_or_string,
    'w': validate_non_negative_int_or_basestring,
    'wtimeoutms': validate_non_negative_integer,
    'zlibcompressionlevel': validate_zlib_compression_level,
}

# Dictionary where keys are the names of URI options specific to pymongo,
# and values are functions that validate user-input values for those options.
NONSPEC_OPTIONS_VALIDATOR_MAP = {
    'connect': validate_boolean_or_string,
    'driver': validate_driver_or_none,
    'fsync': validate_boolean_or_string,
    'minpoolsize': validate_non_negative_integer,
    'socketkeepalive': validate_boolean_or_string,
    'tlscrlfile': validate_readable,
    'tz_aware': validate_boolean_or_string,
    'unicode_decode_error_handler': validate_unicode_decode_error_handler,
    'uuidrepresentation': validate_uuid_representation,
    'waitqueuemultiple': validate_non_negative_integer_or_none,
    'waitqueuetimeoutms': validate_timeout_or_none,
}

# Dictionary where keys are the names of keyword-only options for the
# MongoClient constructor, and values are functions that validate user-input
# values for those options.
KW_VALIDATORS = {
    'document_class': validate_document_class,
    'type_registry': validate_type_registry,
    'read_preference': validate_read_preference,
    'event_listeners': _validate_event_listeners,
    'tzinfo': validate_tzinfo,
    'username': validate_string_or_none,
    'password': validate_string_or_none,
    'server_selector': validate_is_callable_or_none,
    'auto_encryption_opts': validate_auto_encryption_opts_or_none,
}

# Dictionary where keys are any URI option name, and values are the
# internally-used names of that URI option. Options with only one name
# variant need not be included here. Options whose public and internal
# names are the same need not be included here.
INTERNAL_URI_OPTION_NAME_MAP = {
    'j': 'journal',
    'wtimeout': 'wtimeoutms',
    'tls': 'ssl',
    'tlsallowinvalidcertificates': 'ssl_cert_reqs',
    'tlsallowinvalidhostnames': 'ssl_match_hostname',
    'tlscrlfile': 'ssl_crlfile',
    'tlscafile': 'ssl_ca_certs',
    'tlscertificatekeyfile': 'ssl_certfile',
    'tlscertificatekeyfilepassword': 'ssl_pem_passphrase',
}

# Map from deprecated URI option names to a tuple indicating the method of
# their deprecation and any additional information that may be needed to
# construct the warning message.
URI_OPTIONS_DEPRECATION_MAP = {
    # format: <deprecated option name>: (<mode>, <message>),
    # Supported <mode> values:
    # - 'renamed': <message> should be the new option name. Note that case is
    #   preserved for renamed options as they are part of user warnings.
    # - 'removed': <message> may suggest the rationale for deprecating the
    #   option and/or recommend remedial action.
    'j': ('renamed', 'journal'),
    'wtimeout': ('renamed', 'wTimeoutMS'),
    'ssl_cert_reqs': ('renamed', 'tlsAllowInvalidCertificates'),
    'ssl_match_hostname': ('renamed', 'tlsAllowInvalidHostnames'),
    'ssl_crlfile': ('renamed', 'tlsCRLFile'),
    'ssl_ca_certs': ('renamed', 'tlsCAFile'),
    'ssl_pem_passphrase': ('renamed', 'tlsCertificateKeyFilePassword'),
    'waitqueuemultiple': ('removed', (
        'Instead of using waitQueueMultiple to bound queuing, limit the size '
        'of the thread pool in your application server'))
}

# Augment the option validator map with pymongo-specific option information.
URI_OPTIONS_VALIDATOR_MAP.update(NONSPEC_OPTIONS_VALIDATOR_MAP)
for optname, aliases in iteritems(URI_OPTIONS_ALIAS_MAP):
    for alias in aliases:
        if alias not in URI_OPTIONS_VALIDATOR_MAP:
            URI_OPTIONS_VALIDATOR_MAP[alias] = (
                URI_OPTIONS_VALIDATOR_MAP[optname])

# Map containing all URI option and keyword argument validators.
VALIDATORS = URI_OPTIONS_VALIDATOR_MAP.copy()
VALIDATORS.update(KW_VALIDATORS)

# List of timeout-related options.
TIMEOUT_OPTIONS = [
    'connecttimeoutms',
    'heartbeatfrequencyms',
    'maxidletimems',
    'maxstalenessseconds',
    'serverselectiontimeoutms',
    'sockettimeoutms',
    'waitqueuetimeoutms',
]


_AUTH_OPTIONS = frozenset(['authmechanismproperties'])


def validate_auth_option(option, value):
    """Validate optional authentication parameters.
    """
    lower, value = validate(option, value)
    if lower not in _AUTH_OPTIONS:
        raise ConfigurationError('Unknown '
                                 'authentication option: %s' % (option,))
    return lower, value


def validate(option, value):
    """Generic validation function.
    """
    lower = option.lower()
    validator = VALIDATORS.get(lower, raise_config_error)
    value = validator(option, value)
    return lower, value


def get_validated_options(options, warn=True):
    """Validate each entry in options and raise a warning if it is not valid.
    Returns a copy of options with invalid entries removed.

    :Parameters:
        - `opts`: A dict containing MongoDB URI options.
        - `warn` (optional): If ``True`` then warnings will be logged and
          invalid options will be ignored. Otherwise, invalid options will
          cause errors.
    """
    if isinstance(options, _CaseInsensitiveDictionary):
        validated_options = _CaseInsensitiveDictionary()
        get_normed_key = lambda x: x
        get_setter_key = lambda x: options.cased_key(x)
    else:
        validated_options = {}
        get_normed_key = lambda x: x.lower()
        get_setter_key = lambda x: x

    for opt, value in iteritems(options):
        normed_key = get_normed_key(opt)
        try:
            validator = URI_OPTIONS_VALIDATOR_MAP.get(
                normed_key, raise_config_error)
            value = validator(opt, value)
        except (ValueError, TypeError, ConfigurationError) as exc:
            if warn:
                warnings.warn(str(exc))
            else:
                raise
        else:
            validated_options[get_setter_key(normed_key)] = value
    return validated_options


# List of write-concern-related options.
WRITE_CONCERN_OPTIONS = frozenset([
    'w',
    'wtimeout',
    'wtimeoutms',
    'fsync',
    'j',
    'journal'
])


class BaseObject(object):
    """A base class that provides attributes and methods common
    to multiple pymongo classes.

    SHOULD NOT BE USED BY DEVELOPERS EXTERNAL TO MONGODB.
    """

    def __init__(self, codec_options, read_preference, write_concern,
                 read_concern):

        if not isinstance(codec_options, CodecOptions):
            raise TypeError("codec_options must be an instance of "
                            "bson.codec_options.CodecOptions")
        self.__codec_options = codec_options

        if not isinstance(read_preference, _ServerMode):
            raise TypeError("%r is not valid for read_preference. See "
                            "pymongo.read_preferences for valid "
                            "options." % (read_preference,))
        self.__read_preference = read_preference

        if not isinstance(write_concern, WriteConcern):
            raise TypeError("write_concern must be an instance of "
                            "pymongo.write_concern.WriteConcern")
        self.__write_concern = write_concern

        if not isinstance(read_concern, ReadConcern):
            raise TypeError("read_concern must be an instance of "
                            "pymongo.read_concern.ReadConcern")
        self.__read_concern = read_concern

    @property
    def codec_options(self):
        """Read only access to the :class:`~bson.codec_options.CodecOptions`
        of this instance.
        """
        return self.__codec_options

    @property
    def write_concern(self):
        """Read only access to the :class:`~pymongo.write_concern.WriteConcern`
        of this instance.

        .. versionchanged:: 3.0
          The :attr:`write_concern` attribute is now read only.
        """
        return self.__write_concern

    def _write_concern_for(self, session):
        """Read only access to the write concern of this instance or session.
        """
        # Override this operation's write concern with the transaction's.
        if session and session._in_transaction:
            return DEFAULT_WRITE_CONCERN
        return self.write_concern

    @property
    def read_preference(self):
        """Read only access to the read preference of this instance.

        .. versionchanged:: 3.0
          The :attr:`read_preference` attribute is now read only.
        """
        return self.__read_preference

    def _read_preference_for(self, session):
        """Read only access to the read preference of this instance or session.
        """
        # Override this operation's read preference with the transaction's.
        if session:
            return session._txn_read_preference() or self.__read_preference
        return self.__read_preference

    @property
    def read_concern(self):
        """Read only access to the :class:`~pymongo.read_concern.ReadConcern`
        of this instance.

        .. versionadded:: 3.2
        """
        return self.__read_concern


class _CaseInsensitiveDictionary(abc.MutableMapping):
    def __init__(self, *args, **kwargs):
        self.__casedkeys = {}
        self.__data = {}
        self.update(dict(*args, **kwargs))

    def __contains__(self, key):
        return key.lower() in self.__data

    def __len__(self):
        return len(self.__data)

    def __iter__(self):
        return (key for key in self.__casedkeys)

    def __repr__(self):
        return str({self.__casedkeys[k]: self.__data[k] for k in self})

    def __setitem__(self, key, value):
        lc_key = key.lower()
        self.__casedkeys[lc_key] = key
        self.__data[lc_key] = value

    def __getitem__(self, key):
        return self.__data[key.lower()]

    def __delitem__(self, key):
        lc_key = key.lower()
        del self.__casedkeys[lc_key]
        del self.__data[lc_key]

    def __eq__(self, other):
        if not isinstance(other, abc.Mapping):
            return NotImplemented
        if len(self) != len(other):
            return False
        for key in other:
            if self[key] != other[key]:
                return False

        return True

    def get(self, key, default=None):
        return self.__data.get(key.lower(), default)

    def pop(self, key, *args, **kwargs):
        lc_key = key.lower()
        self.__casedkeys.pop(lc_key, None)
        return self.__data.pop(lc_key, *args, **kwargs)

    def popitem(self):
        lc_key, cased_key = self.__casedkeys.popitem()
        value = self.__data.pop(lc_key)
        return cased_key, value

    def clear(self):
        self.__casedkeys.clear()
        self.__data.clear()

    def setdefault(self, key, default=None):
        lc_key = key.lower()
        if key in self:
            return self.__data[lc_key]
        else:
            self.__casedkeys[lc_key] = key
            self.__data[lc_key] = default
            return default

    def update(self, other):
        if isinstance(other, _CaseInsensitiveDictionary):
            for key in other:
                self[other.cased_key(key)] = other[key]
        else:
            for key in other:
                self[key] = other[key]

    def cased_key(self, key):
        return self.__casedkeys[key.lower()]