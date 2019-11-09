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

"""Bits and pieces used by the driver that don't really fit elsewhere."""

import sys
import traceback

from bson.py3compat import abc, iteritems, itervalues, string_type
from bson.son import SON
from pymongo import ASCENDING
from pymongo.errors import (CursorNotFound,
                            DuplicateKeyError,
                            ExecutionTimeout,
                            NotMasterError,
                            OperationFailure,
                            WriteError,
                            WriteConcernError,
                            WTimeoutError)

# From the SDAM spec, the "node is shutting down" codes.
_SHUTDOWN_CODES = frozenset([
    11600,  # InterruptedAtShutdown
    91,     # ShutdownInProgress
])
# From the SDAM spec, the "not master" error codes are combined with the
# "node is recovering" error codes (of which the "node is shutting down"
# errors are a subset).
_NOT_MASTER_CODES = frozenset([
    10107,  # NotMaster
    13435,  # NotMasterNoSlaveOk
    11602,  # InterruptedDueToReplStateChange
    13436,  # NotMasterOrSecondary
    189,    # PrimarySteppedDown
]) | _SHUTDOWN_CODES
# From the retryable writes spec.
_RETRYABLE_ERROR_CODES = _NOT_MASTER_CODES | frozenset([
    7,     # HostNotFound
    6,     # HostUnreachable
    89,    # NetworkTimeout
    9001,  # SocketException
])
_UUNDER = u"_"


def _gen_index_name(keys):
    """Generate an index name from the set of fields it is over."""
    return _UUNDER.join(["%s_%s" % item for item in keys])


def _index_list(key_or_list, direction=None):
    """Helper to generate a list of (key, direction) pairs.

    Takes such a list, or a single key, or a single key and direction.
    """
    if direction is not None:
        return [(key_or_list, direction)]
    else:
        if isinstance(key_or_list, string_type):
            return [(key_or_list, ASCENDING)]
        elif not isinstance(key_or_list, (list, tuple)):
            raise TypeError("if no direction is specified, "
                            "key_or_list must be an instance of list")
        return key_or_list


def _index_document(index_list):
    """Helper to generate an index specifying document.

    Takes a list of (key, direction) pairs.
    """
    if isinstance(index_list, abc.Mapping):
        raise TypeError("passing a dict to sort/create_index/hint is not "
                        "allowed - use a list of tuples instead. did you "
                        "mean %r?" % list(iteritems(index_list)))
    elif not isinstance(index_list, (list, tuple)):
        raise TypeError("must use a list of (key, direction) pairs, "
                        "not: " + repr(index_list))
    if not len(index_list):
        raise ValueError("key_or_list must not be the empty list")

    index = SON()
    for (key, value) in index_list:
        if not isinstance(key, string_type):
            raise TypeError("first item in each key pair must be a string")
        if not isinstance(value, (string_type, int, abc.Mapping)):
            raise TypeError("second item in each key pair must be 1, -1, "
                            "'2d', 'geoHaystack', or another valid MongoDB "
                            "index specifier.")
        index[key] = value
    return index


def _check_command_response(response, msg=None, allowable_errors=None,
                            parse_write_concern_error=False):
    """Check the response to a command for errors.
    """
    if "ok" not in response:
        # Server didn't recognize our message as a command.
        raise OperationFailure(response.get("$err"),
                               response.get("code"),
                               response)

    if parse_write_concern_error and 'writeConcernError' in response:
        _raise_write_concern_error(response['writeConcernError'])

    if not response["ok"]:

        details = response
        # Mongos returns the error details in a 'raw' object
        # for some errors.
        if "raw" in response:
            for shard in itervalues(response["raw"]):
                # Grab the first non-empty raw error from a shard.
                if shard.get("errmsg") and not shard.get("ok"):
                    details = shard
                    break

        errmsg = details["errmsg"]
        if allowable_errors is None or errmsg not in allowable_errors:

            code = details.get("code")
            # Server is "not master" or "recovering"
            if code in _NOT_MASTER_CODES:
                raise NotMasterError(errmsg, response)
            elif ("not master" in errmsg
                  or "node is recovering" in errmsg):
                raise NotMasterError(errmsg, response)

            # Server assertion failures
            if errmsg == "db assertion failure":
                errmsg = ("db assertion failure, assertion: '%s'" %
                          details.get("assertion", ""))
                raise OperationFailure(errmsg,
                                       details.get("assertionCode"),
                                       response)

            # Other errors
            # findAndModify with upsert can raise duplicate key error
            if code in (11000, 11001, 12582):
                raise DuplicateKeyError(errmsg, code, response)
            elif code == 50:
                raise ExecutionTimeout(errmsg, code, response)
            elif code == 43:
                raise CursorNotFound(errmsg, code, response)

            msg = msg or "%s"
            raise OperationFailure(msg % errmsg, code, response)


def _check_gle_response(result):
    """Return getlasterror response as a dict, or raise OperationFailure."""
    # Did getlasterror itself fail?
    _check_command_response(result)

    if result.get("wtimeout", False):
        # MongoDB versions before 1.8.0 return the error message in an "errmsg"
        # field. If "errmsg" exists "err" will also exist set to None, so we
        # have to check for "errmsg" first.
        raise WTimeoutError(result.get("errmsg", result.get("err")),
                            result.get("code"),
                            result)

    error_msg = result.get("err", "")
    if error_msg is None:
        return result

    if error_msg.startswith("not master"):
        raise NotMasterError(error_msg, result)

    details = result

    # mongos returns the error code in an error object for some errors.
    if "errObjects" in result:
        for errobj in result["errObjects"]:
            if errobj.get("err") == error_msg:
                details = errobj
                break

    code = details.get("code")
    if code in (11000, 11001, 12582):
        raise DuplicateKeyError(details["err"], code, result)
    raise OperationFailure(details["err"], code, result)


def _raise_last_write_error(write_errors):
    # If the last batch had multiple errors only report
    # the last error to emulate continue_on_error.
    error = write_errors[-1]
    if error.get("code") == 11000:
        raise DuplicateKeyError(error.get("errmsg"), 11000, error)
    raise WriteError(error.get("errmsg"), error.get("code"), error)


def _raise_write_concern_error(error):
    if "errInfo" in error and error["errInfo"].get('wtimeout'):
        # Make sure we raise WTimeoutError
        raise WTimeoutError(
            error.get("errmsg"), error.get("code"), error)
    raise WriteConcernError(
        error.get("errmsg"), error.get("code"), error)


def _check_write_command_response(result):
    """Backward compatibility helper for write command error handling.
    """
    # Prefer write errors over write concern errors
    write_errors = result.get("writeErrors")
    if write_errors:
        _raise_last_write_error(write_errors)

    error = result.get("writeConcernError")
    if error:
        _raise_write_concern_error(error)


def _raise_last_error(bulk_write_result):
    """Backward compatibility helper for insert error handling.
    """
    # Prefer write errors over write concern errors
    write_errors = bulk_write_result.get("writeErrors")
    if write_errors:
        _raise_last_write_error(write_errors)

    _raise_write_concern_error(bulk_write_result["writeConcernErrors"][-1])


def _fields_list_to_dict(fields, option_name):
    """Takes a sequence of field names and returns a matching dictionary.

    ["a", "b"] becomes {"a": 1, "b": 1}

    and

    ["a.b.c", "d", "a.c"] becomes {"a.b.c": 1, "d": 1, "a.c": 1}
    """
    if isinstance(fields, abc.Mapping):
        return fields

    if isinstance(fields, (abc.Sequence, abc.Set)):
        if not all(isinstance(field, string_type) for field in fields):
            raise TypeError("%s must be a list of key names, each an "
                            "instance of %s" % (option_name,
                                                string_type.__name__))
        return dict.fromkeys(fields, 1)

    raise TypeError("%s must be a mapping or "
                    "list of key names" % (option_name,))


def _handle_exception():
    """Print exceptions raised by subscribers to stderr."""
    # Heavily influenced by logging.Handler.handleError.

    # See note here:
    # https://docs.python.org/3.4/library/sys.html#sys.__stderr__
    if sys.stderr:
        einfo = sys.exc_info()
        try:
            traceback.print_exception(einfo[0], einfo[1], einfo[2],
                                      None, sys.stderr)
        except IOError:
            pass
        finally:
            del einfo
