# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Address objects for network connections.
"""

from __future__ import division, absolute_import

import attr
import warnings, os

from zope.interface import implementer
from twisted.internet.interfaces import IAddress
from twisted.python.filepath import _asFilesystemBytes
from twisted.python.filepath import _coerceToFilesystemEncoding
from twisted.python.runtime import platform
from twisted.python.compat import _PY3


@implementer(IAddress)
@attr.s(hash=True)
class IPv4Address(object):
    """
    An L{IPv4Address} represents the address of an IPv4 socket endpoint.

    @ivar type: A string describing the type of transport, either 'TCP' or
        'UDP'.

    @ivar host: A string containing a dotted-quad IPv4 address; for example,
        "127.0.0.1".
    @type host: C{str}

    @ivar port: An integer representing the port number.
    @type port: C{int}
    """
    type = attr.ib(validator=attr.validators.in_(["TCP", "UDP"]))
    host = attr.ib()
    port = attr.ib()



@implementer(IAddress)
@attr.s(hash=True)
class IPv6Address(object):
    """
    An L{IPv6Address} represents the address of an IPv6 socket endpoint.

    @ivar type: A string describing the type of transport, either 'TCP' or
        'UDP'.

    @ivar host: A string containing a colon-separated, hexadecimal formatted
        IPv6 address; for example, "::1".
    @type host: C{str}

    @ivar port: An integer representing the port number.
    @type port: C{int}

    @ivar flowInfo: the IPv6 flow label.  This can be used by QoS routers to
        identify flows of traffic; you may generally safely ignore it.
    @type flowInfo: L{int}

    @ivar scopeID: the IPv6 scope identifier - roughly analagous to what
        interface traffic destined for this address must be transmitted over.
    @type scopeID: L{int} or L{str}
    """
    type = attr.ib(validator=attr.validators.in_(["TCP", "UDP"]))
    host = attr.ib()
    port = attr.ib()
    flowInfo = attr.ib(default=0)
    scopeID = attr.ib(default=0)



@implementer(IAddress)
class _ProcessAddress(object):
    """
    An L{interfaces.IAddress} provider for process transports.
    """



@attr.s(hash=True)
@implementer(IAddress)
class HostnameAddress(object):
    """
    A L{HostnameAddress} represents the address of a L{HostnameEndpoint}.

    @ivar hostname: A hostname byte string; for example, b"example.com".
    @type hostname: L{bytes}

    @ivar port: An integer representing the port number.
    @type port: L{int}
    """

    hostname = attr.ib()
    port = attr.ib()



@attr.s(hash=False, repr=False, cmp=False)
@implementer(IAddress)
class UNIXAddress(object):
    """
    Object representing a UNIX socket endpoint.

    @ivar name: The filename associated with this socket.
    @type name: C{bytes}
    """

    name = attr.ib(converter=attr.converters.optional(_asFilesystemBytes))

    if getattr(os.path, 'samefile', None) is not None:
        def __eq__(self, other):
            """
            Overriding C{attrs} to ensure the os level samefile
            check is done if the name attributes do not match.
            """
            if isinstance(other, self.__class__):
                res = self.name == other.name
            else:
                return False
            if not res and self.name and other.name:
                try:
                    return os.path.samefile(self.name, other.name)
                except OSError:
                    pass
                except (TypeError, ValueError) as e:
                    # On Linux, abstract namespace UNIX sockets start with a
                    # \0, which os.path doesn't like.
                    if not _PY3 and not platform.isLinux():
                        raise e
            return res
    else:
        def __eq__(self, other):
            if isinstance(other, self.__class__):
                return self.name == other.name
            return False


    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return not self.__eq__(other)
        return True


    def __repr__(self):
        name = self.name
        if name:
            name = _coerceToFilesystemEncoding('', self.name)
        return 'UNIXAddress(%r)' % (name,)


    def __hash__(self):
        if self.name is None:
            return hash((self.__class__, None))
        try:
            s1 = os.stat(self.name)
            return hash((s1.st_ino, s1.st_dev))
        except OSError:
            return hash(self.name)



# These are for buildFactory backwards compatibility due to
# stupidity-induced inconsistency.

class _ServerFactoryIPv4Address(IPv4Address):
    """Backwards compatibility hack. Just like IPv4Address in practice."""

    def __eq__(self, other):
        if isinstance(other, tuple):
            warnings.warn("IPv4Address.__getitem__ is deprecated.  Use attributes instead.",
                          category=DeprecationWarning, stacklevel=2)
            return (self.host, self.port) == other
        elif isinstance(other, IPv4Address):
            a = (self.type, self.host, self.port)
            b = (other.type, other.host, other.port)
            return a == b
        return False
