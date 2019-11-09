"""Internal module for Python 2 backwards compatibility."""
import errno
import socket
import sys


def sendall(sock, *args, **kwargs):
    return sock.sendall(*args, **kwargs)


def shutdown(sock, *args, **kwargs):
    return sock.shutdown(*args, **kwargs)


def ssl_wrap_socket(context, sock, *args, **kwargs):
    return context.wrap_socket(sock, *args, **kwargs)


# For Python older than 3.5, retry EINTR.
if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and
                               sys.version_info[1] < 5):
    # Adapted from https://bugs.python.org/review/23863/patch/14532/54418
    import time

    # Wrapper for handling interruptable system calls.
    def _retryable_call(s, func, *args, **kwargs):
        # Some modules (SSL) use the _fileobject wrapper directly and
        # implement a smaller portion of the socket interface, thus we
        # need to let them continue to do so.
        timeout, deadline = None, 0.0
        attempted = False
        try:
            timeout = s.gettimeout()
        except AttributeError:
            pass

        if timeout:
            deadline = time.time() + timeout

        try:
            while True:
                if attempted and timeout:
                    now = time.time()
                    if now >= deadline:
                        raise socket.error(errno.EWOULDBLOCK, "timed out")
                    else:
                        # Overwrite the timeout on the socket object
                        # to take into account elapsed time.
                        s.settimeout(deadline - now)
                try:
                    attempted = True
                    return func(*args, **kwargs)
                except socket.error as e:
                    if e.args[0] == errno.EINTR:
                        continue
                    raise
        finally:
            # Set the existing timeout back for future
            # calls.
            if timeout:
                s.settimeout(timeout)

    def recv(sock, *args, **kwargs):
        return _retryable_call(sock, sock.recv, *args, **kwargs)

    def recv_into(sock, *args, **kwargs):
        return _retryable_call(sock, sock.recv_into, *args, **kwargs)

else:  # Python 3.5 and above automatically retry EINTR
    def recv(sock, *args, **kwargs):
        return sock.recv(*args, **kwargs)

    def recv_into(sock, *args, **kwargs):
        return sock.recv_into(*args, **kwargs)

if sys.version_info[0] < 3:
    # In Python 3, the ssl module raises socket.timeout whereas it raises
    # SSLError in Python 2. For compatibility between versions, ensure
    # socket.timeout is raised for both.
    import functools

    try:
        from ssl import SSLError as _SSLError
    except ImportError:
        class _SSLError(Exception):
            """A replacement in case ssl.SSLError is not available."""
            pass

    _EXPECTED_SSL_TIMEOUT_MESSAGES = (
        "The handshake operation timed out",
        "The read operation timed out",
        "The write operation timed out",
    )

    def _handle_ssl_timeout(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except _SSLError as e:
                message = len(e.args) == 1 and unicode(e.args[0]) or ''
                if any(x in message for x in _EXPECTED_SSL_TIMEOUT_MESSAGES):
                    # Raise socket.timeout for compatibility with Python 3.
                    raise socket.timeout(*e.args)
                raise
        return wrapper

    recv = _handle_ssl_timeout(recv)
    recv_into = _handle_ssl_timeout(recv_into)
    sendall = _handle_ssl_timeout(sendall)
    shutdown = _handle_ssl_timeout(shutdown)
    ssl_wrap_socket = _handle_ssl_timeout(ssl_wrap_socket)

if sys.version_info[0] < 3:
    from urllib import unquote
    from urlparse import parse_qs, urlparse
    from itertools import imap, izip
    from string import letters as ascii_letters
    from Queue import Queue

    # special unicode handling for python2 to avoid UnicodeDecodeError
    def safe_unicode(obj, *args):
        """ return the unicode representation of obj """
        try:
            return unicode(obj, *args)
        except UnicodeDecodeError:
            # obj is byte string
            ascii_text = str(obj).encode('string_escape')
            return unicode(ascii_text)

    def iteritems(x):
        return x.iteritems()

    def iterkeys(x):
        return x.iterkeys()

    def itervalues(x):
        return x.itervalues()

    def nativestr(x):
        return x if isinstance(x, str) else x.encode('utf-8', 'replace')

    def next(x):
        return x.next()

    def byte_to_chr(x):
        return x

    unichr = unichr
    xrange = xrange
    basestring = basestring
    unicode = unicode
    long = long
    BlockingIOError = socket.error
else:
    from urllib.parse import parse_qs, unquote, urlparse
    from string import ascii_letters
    from queue import Queue

    def iteritems(x):
        return iter(x.items())

    def iterkeys(x):
        return iter(x.keys())

    def itervalues(x):
        return iter(x.values())

    def byte_to_chr(x):
        return chr(x)

    def nativestr(x):
        return x if isinstance(x, str) else x.decode('utf-8', 'replace')

    next = next
    unichr = chr
    imap = map
    izip = zip
    xrange = range
    basestring = str
    unicode = str
    safe_unicode = str
    long = int
    BlockingIOError = BlockingIOError

try:  # Python 3
    from queue import LifoQueue, Empty, Full
except ImportError:  # Python 2
    from Queue import LifoQueue, Empty, Full
