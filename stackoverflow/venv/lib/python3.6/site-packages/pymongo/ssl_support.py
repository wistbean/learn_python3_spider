# Copyright 2014-present MongoDB, Inc.
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

"""Support for SSL in PyMongo."""

import atexit
import sys
import threading

HAVE_SSL = True
try:
    import ssl
except ImportError:
    HAVE_SSL = False

HAVE_CERTIFI = False
try:
    import certifi
    HAVE_CERTIFI = True
except ImportError:
    pass

HAVE_WINCERTSTORE = False
try:
    from wincertstore import CertFile
    HAVE_WINCERTSTORE = True
except ImportError:
    pass

from bson.py3compat import string_type
from pymongo.errors import ConfigurationError

_WINCERTSLOCK = threading.Lock()
_WINCERTS = None

_PY37PLUS = sys.version_info[:2] >= (3, 7)

if HAVE_SSL:
    try:
        # Python 2.7.9+, PyPy 2.5.1+, etc.
        from ssl import SSLContext
    except ImportError:
        from pymongo.ssl_context import SSLContext

    def validate_cert_reqs(option, value):
        """Validate the cert reqs are valid. It must be None or one of the
        three values ``ssl.CERT_NONE``, ``ssl.CERT_OPTIONAL`` or
        ``ssl.CERT_REQUIRED``.
        """
        if value is None:
            return value
        elif isinstance(value, string_type) and hasattr(ssl, value):
            value = getattr(ssl, value)

        if value in (ssl.CERT_NONE, ssl.CERT_OPTIONAL, ssl.CERT_REQUIRED):
            return value
        raise ValueError("The value of %s must be one of: "
                         "`ssl.CERT_NONE`, `ssl.CERT_OPTIONAL` or "
                         "`ssl.CERT_REQUIRED`" % (option,))

    def validate_allow_invalid_certs(option, value):
        """Validate the option to allow invalid certificates is valid."""
        # Avoid circular import.
        from pymongo.common import validate_boolean_or_string
        boolean_cert_reqs = validate_boolean_or_string(option, value)
        if boolean_cert_reqs:
            return ssl.CERT_NONE
        return ssl.CERT_REQUIRED

    def _load_wincerts():
        """Set _WINCERTS to an instance of wincertstore.Certfile."""
        global _WINCERTS

        certfile = CertFile()
        certfile.addstore("CA")
        certfile.addstore("ROOT")
        atexit.register(certfile.close)

        _WINCERTS = certfile

    # XXX: Possible future work.
    # - OCSP? Not supported by python at all.
    #   http://bugs.python.org/issue17123
    # - Adding an ssl_context keyword argument to MongoClient? This might
    #   be useful for sites that have unusual requirements rather than
    #   trying to expose every SSLContext option through a keyword/uri
    #   parameter.
    def get_ssl_context(*args):
        """Create and return an SSLContext object."""
        (certfile,
         keyfile,
         passphrase,
         ca_certs,
         cert_reqs,
         crlfile,
         match_hostname) = args
        verify_mode = ssl.CERT_REQUIRED if cert_reqs is None else cert_reqs
        # Note PROTOCOL_SSLv23 is about the most misleading name imaginable.
        # This configures the server and client to negotiate the
        # highest protocol version they both support. A very good thing.
        # PROTOCOL_TLS_CLIENT was added in CPython 3.6, deprecating
        # PROTOCOL_SSLv23.
        ctx = SSLContext(
            getattr(ssl, "PROTOCOL_TLS_CLIENT", ssl.PROTOCOL_SSLv23))
        # SSLContext.check_hostname was added in CPython 2.7.9 and 3.4.
        # PROTOCOL_TLS_CLIENT (added in Python 3.6) enables it by default.
        if hasattr(ctx, "check_hostname"):
            if _PY37PLUS and verify_mode != ssl.CERT_NONE:
                # Python 3.7 uses OpenSSL's hostname matching implementation
                # making it the obvious version to start using this with.
                # Python 3.6 might have been a good version, but it suffers
                # from https://bugs.python.org/issue32185.
                # We'll use our bundled match_hostname for older Python
                # versions, which also supports IP address matching
                # with Python < 3.5.
                ctx.check_hostname = match_hostname
            else:
                ctx.check_hostname = False
        if hasattr(ctx, "options"):
            # Explicitly disable SSLv2, SSLv3 and TLS compression. Note that
            # up to date versions of MongoDB 2.4 and above already disable
            # SSLv2 and SSLv3, python disables SSLv2 by default in >= 2.7.7
            # and >= 3.3.4 and SSLv3 in >= 3.4.3. There is no way for us to do
            # any of this explicitly for python 2.7 before 2.7.9.
            ctx.options |= getattr(ssl, "OP_NO_SSLv2", 0)
            ctx.options |= getattr(ssl, "OP_NO_SSLv3", 0)
            # OpenSSL >= 1.0.0
            ctx.options |= getattr(ssl, "OP_NO_COMPRESSION", 0)
            # Python 3.7+ with OpenSSL >= 1.1.0h
            ctx.options |= getattr(ssl, "OP_NO_RENEGOTIATION", 0)
        if certfile is not None:
            try:
                if passphrase is not None:
                    vi = sys.version_info
                    # Since python just added a new parameter to an existing method
                    # this seems to be about the best we can do.
                    if (vi[0] == 2 and vi < (2, 7, 9) or
                            vi[0] == 3 and vi < (3, 3)):
                        raise ConfigurationError(
                            "Support for ssl_pem_passphrase requires "
                            "python 2.7.9+ (pypy 2.5.1+) or 3.3+")
                    ctx.load_cert_chain(certfile, keyfile, passphrase)
                else:
                    ctx.load_cert_chain(certfile, keyfile)
            except ssl.SSLError as exc:
                raise ConfigurationError(
                    "Private key doesn't match certificate: %s" % (exc,))
        if crlfile is not None:
            if not hasattr(ctx, "verify_flags"):
                raise ConfigurationError(
                    "Support for ssl_crlfile requires "
                    "python 2.7.9+ (pypy 2.5.1+) or  3.4+")
            # Match the server's behavior.
            ctx.verify_flags = ssl.VERIFY_CRL_CHECK_LEAF
            ctx.load_verify_locations(crlfile)
        if ca_certs is not None:
            ctx.load_verify_locations(ca_certs)
        elif cert_reqs != ssl.CERT_NONE:
            # CPython >= 2.7.9 or >= 3.4.0, pypy >= 2.5.1
            if hasattr(ctx, "load_default_certs"):
                ctx.load_default_certs()
            # Python >= 3.2.0, useless on Windows.
            elif (sys.platform != "win32" and
                  hasattr(ctx, "set_default_verify_paths")):
                ctx.set_default_verify_paths()
            elif sys.platform == "win32" and HAVE_WINCERTSTORE:
                with _WINCERTSLOCK:
                    if _WINCERTS is None:
                        _load_wincerts()
                ctx.load_verify_locations(_WINCERTS.name)
            elif HAVE_CERTIFI:
                ctx.load_verify_locations(certifi.where())
            else:
                raise ConfigurationError(
                    "`ssl_cert_reqs` is not ssl.CERT_NONE and no system "
                    "CA certificates could be loaded. `ssl_ca_certs` is "
                    "required.")
        ctx.verify_mode = verify_mode
        return ctx
else:
    def validate_cert_reqs(option, dummy):
        """No ssl module, raise ConfigurationError."""
        raise ConfigurationError("The value of %s is set but can't be "
                                 "validated. The ssl module is not available"
                                 % (option,))

    def validate_allow_invalid_certs(option, dummy):
        """No ssl module, raise ConfigurationError."""
        return validate_cert_reqs(option, dummy)

    def get_ssl_context(*dummy):
        """No ssl module, raise ConfigurationError."""
        raise ConfigurationError("The ssl module is not available.")
