# -*- coding: utf-8 -*-
u"""Hyperlink provides Pythonic URL parsing, construction, and rendering.

Usage is straightforward::

   >>> from hyperlink import URL
   >>> url = URL.from_text(u'http://github.com/mahmoud/hyperlink?utm_source=docs')
   >>> url.host
   u'github.com'
   >>> secure_url = url.replace(scheme=u'https')
   >>> secure_url.get('utm_source')[0]
   u'docs'

As seen here, the API revolves around the lightweight and immutable
:class:`URL` type, documented below.
"""

import re
import sys
import string
import socket
from unicodedata import normalize
try:
    from socket import inet_pton
except ImportError:
    inet_pton = None  # defined below
try:
    from collections.abc import Mapping
except ImportError:  # Python 2
    from collections import Mapping

# Note: IDNAError is a subclass of UnicodeError
from idna import encode as idna_encode, decode as idna_decode, IDNAError


if inet_pton is None:
    # based on https://gist.github.com/nnemkin/4966028
    # this code only applies on Windows Python 2.7
    import ctypes

    class _sockaddr(ctypes.Structure):
        _fields_ = [("sa_family", ctypes.c_short),
                    ("__pad1", ctypes.c_ushort),
                    ("ipv4_addr", ctypes.c_byte * 4),
                    ("ipv6_addr", ctypes.c_byte * 16),
                    ("__pad2", ctypes.c_ulong)]

    WSAStringToAddressA = ctypes.windll.ws2_32.WSAStringToAddressA
    WSAAddressToStringA = ctypes.windll.ws2_32.WSAAddressToStringA

    def inet_pton(address_family, ip_string):
        addr = _sockaddr()
        ip_string = ip_string.encode('ascii')
        addr.sa_family = address_family
        addr_size = ctypes.c_int(ctypes.sizeof(addr))

        if WSAStringToAddressA(ip_string, address_family, None, ctypes.byref(addr), ctypes.byref(addr_size)) != 0:
            raise socket.error(ctypes.FormatError())

        if address_family == socket.AF_INET:
            return ctypes.string_at(addr.ipv4_addr, 4)
        if address_family == socket.AF_INET6:
            return ctypes.string_at(addr.ipv6_addr, 16)
        raise socket.error('unknown address family')


PY2 = (sys.version_info[0] == 2)
unicode = type(u'')
try:
    unichr
except NameError:
    unichr = chr  # py3
NoneType = type(None)


# from boltons.typeutils
def make_sentinel(name='_MISSING', var_name=None):
    """Creates and returns a new **instance** of a new class, suitable for
    usage as a "sentinel", a kind of singleton often used to indicate
    a value is missing when ``None`` is a valid input.

    Args:
        name (str): Name of the Sentinel
        var_name (str): Set this name to the name of the variable in
            its respective module enable pickleability.

    >>> make_sentinel(var_name='_MISSING')
    _MISSING

    The most common use cases here in boltons are as default values
    for optional function arguments, partly because of its
    less-confusing appearance in automatically generated
    documentation. Sentinels also function well as placeholders in queues
    and linked lists.

    .. note::

      By design, additional calls to ``make_sentinel`` with the same
      values will not produce equivalent objects.

      >>> make_sentinel('TEST') == make_sentinel('TEST')
      False
      >>> type(make_sentinel('TEST')) == type(make_sentinel('TEST'))
      False

    """
    class Sentinel(object):
        def __init__(self):
            self.name = name
            self.var_name = var_name

        def __repr__(self):
            if self.var_name:
                return self.var_name
            return '%s(%r)' % (self.__class__.__name__, self.name)
        if var_name:
            def __reduce__(self):
                return self.var_name

        def __nonzero__(self):
            return False

        __bool__ = __nonzero__

    return Sentinel()


_unspecified = _UNSET = make_sentinel('_UNSET')


# RFC 3986 Section 2.3, Unreserved URI Characters
#   https://tools.ietf.org/html/rfc3986#section-2.3
_UNRESERVED_CHARS = frozenset('~-._0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                              'abcdefghijklmnopqrstuvwxyz')


# URL parsing regex (based on RFC 3986 Appendix B, with modifications)
_URL_RE = re.compile(r'^((?P<scheme>[^:/?#]+):)?'
                     r'((?P<_netloc_sep>//)'
                     r'(?P<authority>[^/?#]*))?'
                     r'(?P<path>[^?#]*)'
                     r'(\?(?P<query>[^#]*))?'
                     r'(#(?P<fragment>.*))?$')
_SCHEME_RE = re.compile(r'^[a-zA-Z0-9+-.]*$')
_AUTHORITY_RE = re.compile(r'^(?:(?P<userinfo>[^@/?#]*)@)?'
                           r'(?P<host>'
                           r'(?:\[(?P<ipv6_host>[^[\]/?#]*)\])'
                           r'|(?P<plain_host>[^:/?#[\]]*)'
                           r'|(?P<bad_host>.*?))?'
                           r'(?::(?P<port>.*))?$')


_HEX_CHAR_MAP = dict([((a + b).encode('ascii'),
                       unichr(int(a + b, 16)).encode('charmap'))
                      for a in string.hexdigits for b in string.hexdigits])
_ASCII_RE = re.compile('([\x00-\x7f]+)')

# RFC 3986 section 2.2, Reserved Characters
#   https://tools.ietf.org/html/rfc3986#section-2.2
_GEN_DELIMS = frozenset(u':/?#[]@')
_SUB_DELIMS = frozenset(u"!$&'()*+,;=")
_ALL_DELIMS = _GEN_DELIMS | _SUB_DELIMS

_USERINFO_SAFE = _UNRESERVED_CHARS | _SUB_DELIMS | set(u'%')
_USERINFO_DELIMS = _ALL_DELIMS - _USERINFO_SAFE
_PATH_SAFE = _USERINFO_SAFE | set(u':@')
_PATH_DELIMS = _ALL_DELIMS - _PATH_SAFE
_SCHEMELESS_PATH_SAFE = _PATH_SAFE - set(':')
_SCHEMELESS_PATH_DELIMS = _ALL_DELIMS - _SCHEMELESS_PATH_SAFE
_FRAGMENT_SAFE = _UNRESERVED_CHARS | _PATH_SAFE | set(u'/?')
_FRAGMENT_DELIMS = _ALL_DELIMS - _FRAGMENT_SAFE
_QUERY_VALUE_SAFE = _UNRESERVED_CHARS | _FRAGMENT_SAFE - set(u'&+')
_QUERY_VALUE_DELIMS = _ALL_DELIMS - _QUERY_VALUE_SAFE
_QUERY_KEY_SAFE = _UNRESERVED_CHARS | _QUERY_VALUE_SAFE - set(u'=')
_QUERY_KEY_DELIMS = _ALL_DELIMS - _QUERY_KEY_SAFE


def _make_decode_map(delims, allow_percent=False):
    ret = dict(_HEX_CHAR_MAP)
    if not allow_percent:
        delims = set(delims) | set([u'%'])
    for delim in delims:
        _hexord = '{0:02X}'.format(ord(delim)).encode('ascii')
        _hexord_lower = _hexord.lower()
        ret.pop(_hexord)
        if _hexord != _hexord_lower:
            ret.pop(_hexord_lower)
    return ret


def _make_quote_map(safe_chars):
    ret = {}
    # v is included in the dict for py3 mostly, because bytestrings
    # are iterables of ints, of course!
    for i, v in zip(range(256), range(256)):
        c = chr(v)
        if c in safe_chars:
            ret[c] = ret[v] = c
        else:
            ret[c] = ret[v] = '%{0:02X}'.format(i)
    return ret


_USERINFO_PART_QUOTE_MAP = _make_quote_map(_USERINFO_SAFE)
_USERINFO_DECODE_MAP = _make_decode_map(_USERINFO_DELIMS)
_PATH_PART_QUOTE_MAP = _make_quote_map(_PATH_SAFE)
_SCHEMELESS_PATH_PART_QUOTE_MAP = _make_quote_map(_SCHEMELESS_PATH_SAFE)
_PATH_DECODE_MAP = _make_decode_map(_PATH_DELIMS)
_QUERY_KEY_QUOTE_MAP = _make_quote_map(_QUERY_KEY_SAFE)
_QUERY_KEY_DECODE_MAP = _make_decode_map(_QUERY_KEY_DELIMS)
_QUERY_VALUE_QUOTE_MAP = _make_quote_map(_QUERY_VALUE_SAFE)
_QUERY_VALUE_DECODE_MAP = _make_decode_map(_QUERY_VALUE_DELIMS)
_FRAGMENT_QUOTE_MAP = _make_quote_map(_FRAGMENT_SAFE)
_FRAGMENT_DECODE_MAP = _make_decode_map(_FRAGMENT_DELIMS)
_UNRESERVED_QUOTE_MAP = _make_quote_map(_UNRESERVED_CHARS)
_UNRESERVED_DECODE_MAP = dict([(k, v) for k, v in _HEX_CHAR_MAP.items()
                               if v.decode('ascii', 'replace')
                               in _UNRESERVED_CHARS])

_ROOT_PATHS = frozenset(((), (u'',)))


def _encode_reserved(text, maximal=True):
    """A very comprehensive percent encoding for encoding all
    delimiters. Used for arguments to DecodedURL, where a % means a
    percent sign, and not the character used by URLs for escaping
    bytes.
    """
    if maximal:
        bytestr = normalize('NFC', text).encode('utf8')
        return u''.join([_UNRESERVED_QUOTE_MAP[b] for b in bytestr])
    return u''.join([_UNRESERVED_QUOTE_MAP[t] if t in _UNRESERVED_CHARS
                     else t for t in text])


def _encode_path_part(text, maximal=True):
    "Percent-encode a single segment of a URL path."
    if maximal:
        bytestr = normalize('NFC', text).encode('utf8')
        return u''.join([_PATH_PART_QUOTE_MAP[b] for b in bytestr])
    return u''.join([_PATH_PART_QUOTE_MAP[t] if t in _PATH_DELIMS else t
                     for t in text])


def _encode_schemeless_path_part(text, maximal=True):
    """Percent-encode the first segment of a URL path for a URL without a
    scheme specified.
    """
    if maximal:
        bytestr = normalize('NFC', text).encode('utf8')
        return u''.join([_SCHEMELESS_PATH_PART_QUOTE_MAP[b] for b in bytestr])
    return u''.join([_SCHEMELESS_PATH_PART_QUOTE_MAP[t]
                     if t in _SCHEMELESS_PATH_DELIMS else t for t in text])


def _encode_path_parts(text_parts, rooted=False, has_scheme=True,
                       has_authority=True, joined=True, maximal=True):
    """
    Percent-encode a tuple of path parts into a complete path.

    Setting *maximal* to False percent-encodes only the reserved
    characters that are syntactically necessary for serialization,
    preserving any IRI-style textual data.

    Leaving *maximal* set to its default True percent-encodes
    everything required to convert a portion of an IRI to a portion of
    a URI.

    RFC 3986 3.3:

       If a URI contains an authority component, then the path component
       must either be empty or begin with a slash ("/") character.  If a URI
       does not contain an authority component, then the path cannot begin
       with two slash characters ("//").  In addition, a URI reference
       (Section 4.1) may be a relative-path reference, in which case the
       first path segment cannot contain a colon (":") character.
    """
    if not text_parts:
        return u'' if joined else text_parts
    if rooted:
        text_parts = (u'',) + text_parts
    # elif has_authority and text_parts:
    #     raise Exception('see rfc above')  # TODO: too late to fail like this?
    encoded_parts = []
    if has_scheme:
        encoded_parts = [_encode_path_part(part, maximal=maximal)
                         if part else part for part in text_parts]
    else:
        encoded_parts = [_encode_schemeless_path_part(text_parts[0])]
        encoded_parts.extend([_encode_path_part(part, maximal=maximal)
                              if part else part for part in text_parts[1:]])
    if joined:
        return u'/'.join(encoded_parts)
    return tuple(encoded_parts)


def _encode_query_key(text, maximal=True):
    """
    Percent-encode a single query string key or value.
    """
    if maximal:
        bytestr = normalize('NFC', text).encode('utf8')
        return u''.join([_QUERY_KEY_QUOTE_MAP[b] for b in bytestr])
    return u''.join([_QUERY_KEY_QUOTE_MAP[t] if t in _QUERY_KEY_DELIMS else t
                     for t in text])


def _encode_query_value(text, maximal=True):
    """
    Percent-encode a single query string key or value.
    """
    if maximal:
        bytestr = normalize('NFC', text).encode('utf8')
        return u''.join([_QUERY_VALUE_QUOTE_MAP[b] for b in bytestr])
    return u''.join([_QUERY_VALUE_QUOTE_MAP[t]
                     if t in _QUERY_VALUE_DELIMS else t for t in text])


def _encode_fragment_part(text, maximal=True):
    """Quote the fragment part of the URL. Fragments don't have
    subdelimiters, so the whole URL fragment can be passed.
    """
    if maximal:
        bytestr = normalize('NFC', text).encode('utf8')
        return u''.join([_FRAGMENT_QUOTE_MAP[b] for b in bytestr])
    return u''.join([_FRAGMENT_QUOTE_MAP[t] if t in _FRAGMENT_DELIMS else t
                     for t in text])


def _encode_userinfo_part(text, maximal=True):
    """Quote special characters in either the username or password
    section of the URL.
    """
    if maximal:
        bytestr = normalize('NFC', text).encode('utf8')
        return u''.join([_USERINFO_PART_QUOTE_MAP[b] for b in bytestr])
    return u''.join([_USERINFO_PART_QUOTE_MAP[t] if t in _USERINFO_DELIMS
                     else t for t in text])



# This port list painstakingly curated by hand searching through
# https://www.iana.org/assignments/uri-schemes/uri-schemes.xhtml
# and
# https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml
SCHEME_PORT_MAP = {'acap': 674, 'afp': 548, 'dict': 2628, 'dns': 53,
                   'file': None, 'ftp': 21, 'git': 9418, 'gopher': 70,
                   'http': 80, 'https': 443, 'imap': 143, 'ipp': 631,
                   'ipps': 631, 'irc': 194, 'ircs': 6697, 'ldap': 389,
                   'ldaps': 636, 'mms': 1755, 'msrp': 2855, 'msrps': None,
                   'mtqp': 1038, 'nfs': 111, 'nntp': 119, 'nntps': 563,
                   'pop': 110, 'prospero': 1525, 'redis': 6379, 'rsync': 873,
                   'rtsp': 554, 'rtsps': 322, 'rtspu': 5005, 'sftp': 22,
                   'smb': 445, 'snmp': 161, 'ssh': 22, 'steam': None,
                   'svn': 3690, 'telnet': 23, 'ventrilo': 3784, 'vnc': 5900,
                   'wais': 210, 'ws': 80, 'wss': 443, 'xmpp': None}

# This list of schemes that don't use authorities is also from the link above.
NO_NETLOC_SCHEMES = set(['urn', 'about', 'bitcoin', 'blob', 'data', 'geo',
                         'magnet', 'mailto', 'news', 'pkcs11',
                         'sip', 'sips', 'tel'])
# As of Mar 11, 2017, there were 44 netloc schemes, and 13 non-netloc


def register_scheme(text, uses_netloc=True, default_port=None):
    """Registers new scheme information, resulting in correct port and
    slash behavior from the URL object. There are dozens of standard
    schemes preregistered, so this function is mostly meant for
    proprietary internal customizations or stopgaps on missing
    standards information. If a scheme seems to be missing, please
    `file an issue`_!

    Args:
        text (unicode): Text representing the scheme.
           (the 'http' in 'http://hatnote.com')
        uses_netloc (bool): Does the scheme support specifying a
           network host? For instance, "http" does, "mailto" does
           not. Defaults to True.
        default_port (int): The default port, if any, for netloc-using
           schemes.

    .. _file an issue: https://github.com/mahmoud/hyperlink/issues

    """
    text = text.lower()
    if default_port is not None:
        try:
            default_port = int(default_port)
        except (ValueError, TypeError):
            raise ValueError('default_port expected integer or None, not %r'
                             % (default_port,))

    if uses_netloc is True:
        SCHEME_PORT_MAP[text] = default_port
    elif uses_netloc is False:
        if default_port is not None:
            raise ValueError('unexpected default port while specifying'
                             ' non-netloc scheme: %r' % default_port)
        NO_NETLOC_SCHEMES.add(text)
    else:
        raise ValueError('uses_netloc expected bool, not: %r' % uses_netloc)

    return


def scheme_uses_netloc(scheme, default=None):
    """Whether or not a URL uses :code:`:` or :code:`://` to separate the
    scheme from the rest of the URL depends on the scheme's own
    standard definition. There is no way to infer this behavior
    from other parts of the URL. A scheme either supports network
    locations or it does not.

    The URL type's approach to this is to check for explicitly
    registered schemes, with common schemes like HTTP
    preregistered. This is the same approach taken by
    :mod:`urlparse`.

    URL adds two additional heuristics if the scheme as a whole is
    not registered. First, it attempts to check the subpart of the
    scheme after the last ``+`` character. This adds intuitive
    behavior for schemes like ``git+ssh``. Second, if a URL with
    an unrecognized scheme is loaded, it will maintain the
    separator it sees.
    """
    if not scheme:
        return False
    scheme = scheme.lower()
    if scheme in SCHEME_PORT_MAP:
        return True
    if scheme in NO_NETLOC_SCHEMES:
        return False
    if scheme.split('+')[-1] in SCHEME_PORT_MAP:
        return True
    return default


class URLParseError(ValueError):
    """Exception inheriting from :exc:`ValueError`, raised when failing to
    parse a URL. Mostly raised on invalid ports and IPv6 addresses.
    """
    pass


def _optional(argument, default):
    if argument is _UNSET:
        return default
    else:
        return argument


def _typecheck(name, value, *types):
    """
    Check that the given *value* is one of the given *types*, or raise an
    exception describing the problem using *name*.
    """
    if not types:
        raise ValueError('expected one or more types, maybe use _textcheck?')
    if not isinstance(value, types):
        raise TypeError("expected %s for %s, got %r"
                        % (" or ".join([t.__name__ for t in types]),
                           name, value))
    return value


def _textcheck(name, value, delims=frozenset(), nullable=False):
    if not isinstance(value, unicode):
        if nullable and value is None:
            return value  # used by query string values
        else:
            str_name = "unicode" if PY2 else "str"
            exp = str_name + ' or NoneType' if nullable else str_name
            raise TypeError('expected %s for %s, got %r' % (exp, name, value))
    if delims and set(value) & set(delims):  # TODO: test caching into regexes
        raise ValueError('one or more reserved delimiters %s present in %s: %r'
                         % (''.join(delims), name, value))
    return value


def iter_pairs(iterable):
    """
    Iterate over the (key, value) pairs in ``iterable``.

    This handles dictionaries sensibly, and falls back to assuming the
    iterable yields (key, value) pairs. This behaviour is similar to
    what Python's ``dict()`` constructor does.
    """
    if isinstance(iterable, Mapping):
        iterable = iterable.items()
    return iter(iterable)


def _decode_unreserved(text, normalize_case=False, encode_stray_percents=False):
    return _percent_decode(text, normalize_case=normalize_case,
                           encode_stray_percents=encode_stray_percents,
                           _decode_map=_UNRESERVED_DECODE_MAP)


def _decode_userinfo_part(text, normalize_case=False, encode_stray_percents=False):
    return _percent_decode(text, normalize_case=normalize_case,
                           encode_stray_percents=encode_stray_percents,
                           _decode_map=_USERINFO_DECODE_MAP)


def _decode_path_part(text, normalize_case=False, encode_stray_percents=False):
    """
    >>> _decode_path_part(u'%61%77%2f%7a')
    u'aw%2fz'
    >>> _decode_path_part(u'%61%77%2f%7a', normalize_case=True)
    u'aw%2Fz'
    """
    return _percent_decode(text, normalize_case=normalize_case,
                           encode_stray_percents=encode_stray_percents,
                           _decode_map=_PATH_DECODE_MAP)


def _decode_query_key(text, normalize_case=False, encode_stray_percents=False):
    return _percent_decode(text, normalize_case=normalize_case,
                           encode_stray_percents=encode_stray_percents,
                           _decode_map=_QUERY_KEY_DECODE_MAP)


def _decode_query_value(text, normalize_case=False, encode_stray_percents=False):
    return _percent_decode(text, normalize_case=normalize_case,
                           encode_stray_percents=encode_stray_percents,
                           _decode_map=_QUERY_VALUE_DECODE_MAP)


def _decode_fragment_part(text, normalize_case=False, encode_stray_percents=False):
    return _percent_decode(text, normalize_case=normalize_case,
                           encode_stray_percents=encode_stray_percents,
                           _decode_map=_FRAGMENT_DECODE_MAP)


def _percent_decode(text, normalize_case=False, subencoding='utf-8',
                    raise_subencoding_exc=False, encode_stray_percents=False,
                    _decode_map=_HEX_CHAR_MAP):
    """Convert percent-encoded text characters to their normal,
    human-readable equivalents.

    All characters in the input text must be encodable by
    *subencoding*. All special characters underlying the values in the
    percent-encoding must be decodable as *subencoding*. If a
    non-*subencoding*-valid string is passed, the original text is
    returned with no changes applied.

    Only called by field-tailored variants, e.g.,
    :func:`_decode_path_part`, as every percent-encodable part of the
    URL has characters which should not be percent decoded.

    >>> _percent_decode(u'abc%20def')
    u'abc def'

    Args:
       text (unicode): Text with percent-encoding present.
       normalize_case (bool): Whether undecoded percent segments, such
          as encoded delimiters, should be uppercased, per RFC 3986
          Section 2.1. See :func:`_decode_path_part` for an example.
       subencoding (unicode): The name of the encoding underlying the
          percent-encoding. Pass `False` to get back raw bytes.
       raise_subencoding_exc (bool): Whether an error in decoding the bytes
          underlying the percent-decoding should be raised.

    Returns:
       unicode: The percent-decoded version of *text*, decoded by
         *subencoding*, unless `subencoding=False` which returns bytes.

    """
    try:
        quoted_bytes = text.encode('utf-8' if subencoding is False else subencoding)
    except UnicodeEncodeError:
        return text

    bits = quoted_bytes.split(b'%')
    if len(bits) == 1:
        return text

    res = [bits[0]]
    append = res.append

    for item in bits[1:]:
        hexpair, rest = item[:2], item[2:]
        try:
            append(_decode_map[hexpair])
            append(rest)
        except KeyError:
            pair_is_hex = hexpair in _HEX_CHAR_MAP
            if pair_is_hex or not encode_stray_percents:
                append(b'%')
            else:
                # if it's undecodable, treat as a real percent sign,
                # which is reserved (because it wasn't in the
                # context-aware _decode_map passed in), and should
                # stay in an encoded state.
                append(b'%25')
            if normalize_case and pair_is_hex:
                append(hexpair.upper())
                append(rest)
            else:
                append(item)

    unquoted_bytes = b''.join(res)

    if subencoding is False:
        return unquoted_bytes
    try:
        return unquoted_bytes.decode(subencoding)
    except UnicodeDecodeError:
        if raise_subencoding_exc:
            raise
        return text


def _decode_host(host):
    """Decode a host from ASCII-encodable text to IDNA-decoded text. If
    the host text is not ASCII, it is returned unchanged, as it is
    presumed that it is already IDNA-decoded.

    Some technical details: _decode_host is built on top of the "idna"
    package, which has some quirks:

    Capital letters are not valid IDNA2008. The idna package will
    raise an exception like this on capital letters:

    > idna.core.InvalidCodepoint: Codepoint U+004B at position 1 ... not allowed

    However, if a segment of a host (i.e., something in
    url.host.split('.')) is already ASCII, idna doesn't perform its
    usual checks. In fact, for capital letters it automatically
    lowercases them.

    This check and some other functionality can be bypassed by passing
    uts46=True to idna.encode/decode. This allows a more permissive and
    convenient interface. So far it seems like the balanced approach.

    Example output (from idna==2.6):

    >> idna.encode(u'mahmöud.io')
    'xn--mahmud-zxa.io'
    >> idna.encode(u'Mahmöud.io')
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "/home/mahmoud/virtualenvs/hyperlink/local/lib/python2.7/site-packages/idna/core.py", line 355, in encode
        result.append(alabel(label))
      File "/home/mahmoud/virtualenvs/hyperlink/local/lib/python2.7/site-packages/idna/core.py", line 276, in alabel
        check_label(label)
      File "/home/mahmoud/virtualenvs/hyperlink/local/lib/python2.7/site-packages/idna/core.py", line 253, in check_label
        raise InvalidCodepoint('Codepoint {0} at position {1} of {2} not allowed'.format(_unot(cp_value), pos+1, repr(label)))
    idna.core.InvalidCodepoint: Codepoint U+004D at position 1 of u'Mahm\xf6ud' not allowed
    >> idna.encode(u'Mahmoud.io')
    'Mahmoud.io'

    # Similar behavior for decodes below
    >> idna.decode(u'Mahmoud.io')
    u'mahmoud.io
    >> idna.decode(u'Méhmoud.io', uts46=True)
    u'm\xe9hmoud.io'
    """
    if not host:
        return u''
    try:
        host_bytes = host.encode("ascii")
    except UnicodeEncodeError:
        host_text = host
    else:
        try:
            host_text = idna_decode(host_bytes, uts46=True)
        except ValueError:
            # only reached on "narrow" (UCS-2) Python builds <3.4, see #7
            # NOTE: not going to raise here, because there's no
            # ambiguity in the IDNA, and the host is still
            # technically usable
            host_text = host
    return host_text


def _resolve_dot_segments(path):
    """Normalize the URL path by resolving segments of '.' and '..'. For
    more details, see `RFC 3986 section 5.2.4, Remove Dot Segments`_.

    Args:
       path (list): path segments in string form

    Returns:
       list: a new list of path segments with the '.' and '..' elements
          removed and resolved.

    .. _RFC 3986 section 5.2.4, Remove Dot Segments: https://tools.ietf.org/html/rfc3986#section-5.2.4
    """
    segs = []

    for seg in path:
        if seg == u'.':
            pass
        elif seg == u'..':
            if segs:
                segs.pop()
        else:
            segs.append(seg)

    if list(path[-1:]) in ([u'.'], [u'..']):
        segs.append(u'')

    return segs


def parse_host(host):
    """Parse the host into a tuple of ``(family, host)``, where family
    is the appropriate :mod:`socket` module constant when the host is
    an IP address. Family is ``None`` when the host is not an IP.

    Will raise :class:`URLParseError` on invalid IPv6 constants.

    Returns:
      tuple: family (socket constant or None), host (string)

    >>> parse_host('googlewebsite.com') == (None, 'googlewebsite.com')
    True
    >>> parse_host('::1') == (socket.AF_INET6, '::1')
    True
    >>> parse_host('192.168.1.1') == (socket.AF_INET, '192.168.1.1')
    True
    """
    if not host:
        return None, u''
    if u':' in host:
        try:
            inet_pton(socket.AF_INET6, host)
        except socket.error as se:
            raise URLParseError('invalid IPv6 host: %r (%r)' % (host, se))
        except UnicodeEncodeError:
            pass  # TODO: this can't be a real host right?
        else:
            family = socket.AF_INET6
            return family, host
    try:
        inet_pton(socket.AF_INET, host)
    except (socket.error, UnicodeEncodeError):
        family = None  # not an IP
    else:
        family = socket.AF_INET
    return family, host


class URL(object):
    """From blogs to billboards, URLs are so common, that it's easy to
    overlook their complexity and power. With hyperlink's
    :class:`URL` type, working with URLs doesn't have to be hard.

    URLs are made of many parts. Most of these parts are officially
    named in `RFC 3986`_ and this diagram may prove handy in identifying
    them::

       foo://user:pass@example.com:8042/over/there?name=ferret#nose
       \_/   \_______/ \_________/ \__/\_________/ \_________/ \__/
        |        |          |        |      |           |        |
      scheme  userinfo     host     port   path       query   fragment

    While :meth:`~URL.from_text` is used for parsing whole URLs, the
    :class:`URL` constructor builds a URL from the individual
    components, like so::

        >>> from hyperlink import URL
        >>> url = URL(scheme=u'https', host=u'example.com', path=[u'hello', u'world'])
        >>> print(url.to_text())
        https://example.com/hello/world

    The constructor runs basic type checks. All strings are expected
    to be decoded (:class:`unicode` in Python 2). All arguments are
    optional, defaulting to appropriately empty values. A full list of
    constructor arguments is below.

    Args:
       scheme (unicode): The text name of the scheme.
       host (unicode): The host portion of the network location
       port (int): The port part of the network location. If
          ``None`` or no port is passed, the port will default to
          the default port of the scheme, if it is known. See the
          ``SCHEME_PORT_MAP`` and :func:`register_default_port`
          for more info.
       path (tuple): A tuple of strings representing the
          slash-separated parts of the path.
       query (tuple): The query parameters, as a dictionary or
          as an iterable of key-value pairs.
       fragment (unicode): The fragment part of the URL.
       rooted (bool): Whether or not the path begins with a slash.
       userinfo (unicode): The username or colon-separated
          username:password pair.
       uses_netloc (bool): Indicates whether two slashes appear
          between the scheme and the host (``http://eg.com`` vs
          ``mailto:e@g.com``). Set automatically based on scheme.

    All of these parts are also exposed as read-only attributes of
    URL instances, along with several useful methods.

    .. _RFC 3986: https://tools.ietf.org/html/rfc3986
    .. _RFC 3987: https://tools.ietf.org/html/rfc3987
    """

    def __init__(self, scheme=None, host=None, path=(), query=(), fragment=u'',
                 port=None, rooted=None, userinfo=u'', uses_netloc=None):
        if host is not None and scheme is None:
            scheme = u'http'  # TODO: why
        if port is None:
            port = SCHEME_PORT_MAP.get(scheme)
        if host and query and not path:
            # per RFC 3986 6.2.3, "a URI that uses the generic syntax
            # for authority with an empty path should be normalized to
            # a path of '/'."
            path = (u'',)

        # Now that we're done detecting whether they were passed, we can set
        # them to their defaults:
        if scheme is None:
            scheme = u''
        if host is None:
            host = u''
        if rooted is None:
            rooted = bool(host)

        # Set attributes.
        self._scheme = _textcheck("scheme", scheme)
        if self._scheme:
            if not _SCHEME_RE.match(self._scheme):
                raise ValueError('invalid scheme: %r. Only alphanumeric, "+",'
                                 ' "-", and "." allowed. Did you meant to call'
                                 ' %s.from_text()?'
                                 % (self._scheme, self.__class__.__name__))

        _, self._host = parse_host(_textcheck('host', host, '/?#@'))
        if isinstance(path, unicode):
            raise TypeError("expected iterable of text for path, not: %r"
                            % (path,))
        self._path = tuple((_textcheck("path segment", segment, '/?#')
                            for segment in path))
        self._query = tuple(
            (_textcheck("query parameter name", k, '&=#'),
             _textcheck("query parameter value", v, '&#', nullable=True))
            for k, v in iter_pairs(query))
        self._fragment = _textcheck("fragment", fragment)
        self._port = _typecheck("port", port, int, NoneType)
        self._rooted = _typecheck("rooted", rooted, bool)
        self._userinfo = _textcheck("userinfo", userinfo, '/?#@')

        uses_netloc = scheme_uses_netloc(self._scheme, uses_netloc)
        self._uses_netloc = _typecheck("uses_netloc",
                                       uses_netloc, bool, NoneType)

        return

    def get_decoded_url(self, lazy=False):
        try:
            return self._decoded_url
        except AttributeError:
            self._decoded_url = DecodedURL(self, lazy=lazy)
        return self._decoded_url

    @property
    def scheme(self):
        """The scheme is a string, and the first part of an absolute URL, the
        part before the first colon, and the part which defines the
        semantics of the rest of the URL. Examples include "http",
        "https", "ssh", "file", "mailto", and many others. See
        :func:`~hyperlink.register_scheme()` for more info.
        """
        return self._scheme

    @property
    def host(self):
        """The host is a string, and the second standard part of an absolute
        URL. When present, a valid host must be a domain name, or an
        IP (v4 or v6). It occurs before the first slash, or the second
        colon, if a :attr:`~hyperlink.URL.port` is provided.
        """
        return self._host

    @property
    def port(self):
        """The port is an integer that is commonly used in connecting to the
        :attr:`host`, and almost never appears without it.

        When not present in the original URL, this attribute defaults
        to the scheme's default port. If the scheme's default port is
        not known, and the port is not provided, this attribute will
        be set to None.

        >>> URL.from_text(u'http://example.com/pa/th').port
        80
        >>> URL.from_text(u'foo://example.com/pa/th').port
        >>> URL.from_text(u'foo://example.com:8042/pa/th').port
        8042

        .. note::

           Per the standard, when the port is the same as the schemes
           default port, it will be omitted in the text URL.

        """
        return self._port

    @property
    def path(self):
        """A tuple of strings, created by splitting the slash-separated
        hierarchical path. Started by the first slash after the host,
        terminated by a "?", which indicates the start of the
        :attr:`~hyperlink.URL.query` string.
        """
        return self._path

    @property
    def query(self):
        """Tuple of pairs, created by splitting the ampersand-separated
        mapping of keys and optional values representing
        non-hierarchical data used to identify the resource. Keys are
        always strings. Values are strings when present, or None when
        missing.

        For more operations on the mapping, see
        :meth:`~hyperlink.URL.get()`, :meth:`~hyperlink.URL.add()`,
        :meth:`~hyperlink.URL.set()`, and
        :meth:`~hyperlink.URL.delete()`.
        """
        return self._query

    @property
    def fragment(self):
        """A string, the last part of the URL, indicated by the first "#"
        after the :attr:`~hyperlink.URL.path` or
        :attr:`~hyperlink.URL.query`. Enables indirect identification
        of a secondary resource, like an anchor within an HTML page.

        """
        return self._fragment

    @property
    def rooted(self):
        """Whether or not the path starts with a forward slash (``/``).

        This is taken from the terminology in the BNF grammar,
        specifically the "path-rootless", rule, since "absolute path"
        and "absolute URI" are somewhat ambiguous. :attr:`path` does
        not contain the implicit prefixed ``"/"`` since that is
        somewhat awkward to work with.

        """
        return self._rooted

    @property
    def userinfo(self):
        """The colon-separated string forming the username-password
        combination.
        """
        return self._userinfo

    @property
    def uses_netloc(self):
        """
        """
        return self._uses_netloc

    @property
    def user(self):
        """
        The user portion of :attr:`~hyperlink.URL.userinfo`.
        """
        return self.userinfo.split(u':')[0]

    def authority(self, with_password=False, **kw):
        """Compute and return the appropriate host/port/userinfo combination.

        >>> url = URL.from_text(u'http://user:pass@localhost:8080/a/b?x=y')
        >>> url.authority()
        u'user:@localhost:8080'
        >>> url.authority(with_password=True)
        u'user:pass@localhost:8080'

        Args:
           with_password (bool): Whether the return value of this
              method include the password in the URL, if it is
              set. Defaults to False.

        Returns:
           str: The authority (network location and user information) portion
              of the URL.
        """
        # first, a bit of twisted compat
        with_password = kw.pop('includeSecrets', with_password)
        if kw:
            raise TypeError('got unexpected keyword arguments: %r' % kw.keys())
        host = self.host
        if ':' in host:
            hostport = ['[' + host + ']']
        else:
            hostport = [self.host]
        if self.port != SCHEME_PORT_MAP.get(self.scheme):
            hostport.append(unicode(self.port))
        authority = []
        if self.userinfo:
            userinfo = self.userinfo
            if not with_password and u":" in userinfo:
                userinfo = userinfo[:userinfo.index(u":") + 1]
            authority.append(userinfo)
        authority.append(u":".join(hostport))
        return u"@".join(authority)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        for attr in ['scheme', 'userinfo', 'host', 'query',
                     'fragment', 'port', 'uses_netloc']:
            if getattr(self, attr) != getattr(other, attr):
                return False
        if self.path == other.path or (self.path in _ROOT_PATHS
                                       and other.path in _ROOT_PATHS):
            return True
        return False

    def __ne__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.__class__, self.scheme, self.userinfo, self.host,
                     self.path, self.query, self.fragment, self.port,
                     self.rooted, self.uses_netloc))

    @property
    def absolute(self):
        """Whether or not the URL is "absolute". Absolute URLs are complete
        enough to resolve to a network resource without being relative
        to a base URI.

        >>> URL.from_text(u'http://wikipedia.org/').absolute
        True
        >>> URL.from_text(u'?a=b&c=d').absolute
        False

        Absolute URLs must have both a scheme and a host set.
        """
        return bool(self.scheme and self.host)

    def replace(self, scheme=_UNSET, host=_UNSET, path=_UNSET, query=_UNSET,
                fragment=_UNSET, port=_UNSET, rooted=_UNSET, userinfo=_UNSET,
                uses_netloc=_UNSET):
        """:class:`URL` objects are immutable, which means that attributes
        are designed to be set only once, at construction. Instead of
        modifying an existing URL, one simply creates a copy with the
        desired changes.

        If any of the following arguments is omitted, it defaults to
        the value on the current URL.

        Args:
           scheme (unicode): The text name of the scheme.
           host (unicode): The host portion of the network location
           port (int): The port part of the network location.
           path (tuple): A tuple of strings representing the
              slash-separated parts of the path.
           query (tuple): The query parameters, as a tuple of
              key-value pairs.
           query (tuple): The query parameters, as a dictionary or
              as an iterable of key-value pairs.
           fragment (unicode): The fragment part of the URL.
           rooted (bool): Whether or not the path begins with a slash.
           userinfo (unicode): The username or colon-separated
              username:password pair.
           uses_netloc (bool): Indicates whether two slashes appear
              between the scheme and the host (``http://eg.com`` vs
              ``mailto:e@g.com``)

        Returns:
           URL: a copy of the current :class:`URL`, with new values for
              parameters passed.

        """
        return self.__class__(
            scheme=_optional(scheme, self.scheme),
            host=_optional(host, self.host),
            path=_optional(path, self.path),
            query=_optional(query, self.query),
            fragment=_optional(fragment, self.fragment),
            port=_optional(port, self.port),
            rooted=_optional(rooted, self.rooted),
            userinfo=_optional(userinfo, self.userinfo),
            uses_netloc=_optional(uses_netloc, self.uses_netloc)
        )

    @classmethod
    def from_text(cls, text):
        """Whereas the :class:`URL` constructor is useful for constructing
        URLs from parts, :meth:`~URL.from_text` supports parsing whole
        URLs from their string form::

           >>> URL.from_text(u'http://example.com')
           URL.from_text(u'http://example.com')
           >>> URL.from_text(u'?a=b&x=y')
           URL.from_text(u'?a=b&x=y')

        As you can see above, it's also used as the :func:`repr` of
        :class:`URL` objects. The natural counterpart to
        :func:`~URL.to_text()`. This method only accepts *text*, so be
        sure to decode those bytestrings.

        Args:
           text (unicode): A valid URL string.

        Returns:
           URL: The structured object version of the parsed string.

        .. note::

            Somewhat unexpectedly, URLs are a far more permissive
            format than most would assume. Many strings which don't
            look like URLs are still valid URLs. As a result, this
            method only raises :class:`URLParseError` on invalid port
            and IPv6 values in the host portion of the URL.

        """
        um = _URL_RE.match(_textcheck('text', text))
        try:
            gs = um.groupdict()
        except AttributeError:
            raise URLParseError('could not parse url: %r' % text)

        au_text = gs['authority'] or u''
        au_m = _AUTHORITY_RE.match(au_text)
        try:
            au_gs = au_m.groupdict()
        except AttributeError:
            raise URLParseError('invalid authority %r in url: %r'
                                % (au_text, text))
        if au_gs['bad_host']:
            raise URLParseError('invalid host %r in url: %r'
                               % (au_gs['bad_host'], text))

        userinfo = au_gs['userinfo'] or u''

        host = au_gs['ipv6_host'] or au_gs['plain_host']
        port = au_gs['port']
        if port is not None:
            try:
                port = int(port)
            except ValueError:
                if not port:  # TODO: excessive?
                    raise URLParseError('port must not be empty: %r' % au_text)
                raise URLParseError('expected integer for port, not %r' % port)

        scheme = gs['scheme'] or u''
        fragment = gs['fragment'] or u''
        uses_netloc = bool(gs['_netloc_sep'])

        if gs['path']:
            path = gs['path'].split(u"/")
            if not path[0]:
                path.pop(0)
                rooted = True
            else:
                rooted = False
        else:
            path = ()
            rooted = bool(au_text)
        if gs['query']:
            query = ((qe.split(u"=", 1) if u'=' in qe else (qe, None))
                     for qe in gs['query'].split(u"&"))
        else:
            query = ()
        return cls(scheme, host, path, query, fragment, port,
                   rooted, userinfo, uses_netloc)

    def normalize(self, scheme=True, host=True, path=True, query=True,
                  fragment=True, userinfo=True, percents=True):
        """Return a new URL object with several standard normalizations
        applied:

        * Decode unreserved characters (`RFC 3986 2.3`_)
        * Uppercase remaining percent-encoded octets (`RFC 3986 2.1`_)
        * Convert scheme and host casing to lowercase (`RFC 3986 3.2.2`_)
        * Resolve any "." and ".." references in the path (`RFC 3986 6.2.2.3`_)
        * Ensure an ending slash on URLs with an empty path (`RFC 3986 6.2.3`_)
        * Encode any stray percent signs (`%`) in percent-encoded
          fields (path, query, fragment, userinfo) (`RFC 3986 2.4`_)

        All are applied by default, but normalizations can be disabled
        per-part by passing `False` for that part's corresponding
        name.

        Args:
           scheme (bool): Convert the scheme to lowercase
           host (bool): Convert the host to lowercase
           path (bool): Normalize the path (see above for details)
           query (bool): Normalize the query string
           fragment (bool): Normalize the fragment
           userinfo (bool): Normalize the userinfo
           percents (bool): Encode isolated percent signs
              for any percent-encoded fields which are being
              normalized (defaults to True).

        >>> url = URL.from_text(u'Http://example.COM/a/../b/./c%2f?%61%')
        >>> print(url.normalize().to_text())
        http://example.com/b/c%2F?a%25

        .. _RFC 3986 3.2.2: https://tools.ietf.org/html/rfc3986#section-3.2.2
        .. _RFC 3986 2.3: https://tools.ietf.org/html/rfc3986#section-2.3
        .. _RFC 3986 2.1: https://tools.ietf.org/html/rfc3986#section-2.1
        .. _RFC 3986 6.2.2.3: https://tools.ietf.org/html/rfc3986#section-6.2.2.3
        .. _RFC 3986 6.2.3: https://tools.ietf.org/html/rfc3986#section-6.2.3
        .. _RFC 3986 2.4: https://tools.ietf.org/html/rfc3986#section-2.4

        """
        kw = {}
        if scheme:
            kw['scheme'] = self.scheme.lower()
        if host:
            kw['host'] = self.host.lower()
        def _dec_unres(target):
            return _decode_unreserved(target, normalize_case=True,
                                      encode_stray_percents=percents)
        if path:
            if self.path:
                kw['path'] = [_dec_unres(p) for p in _resolve_dot_segments(self.path)]
            else:
                kw['path'] = (u'',)
        if query:
            kw['query'] = [(_dec_unres(k), _dec_unres(v) if v else v)
                           for k, v in self.query]
        if fragment:
            kw['fragment'] = _dec_unres(self.fragment)
        if userinfo:
            kw['userinfo'] = u':'.join([_dec_unres(p)
                                        for p in self.userinfo.split(':', 1)])

        return self.replace(**kw)

    def child(self, *segments):
        """Make a new :class:`URL` where the given path segments are a child
        of this URL, preserving other parts of the URL, including the
        query string and fragment.

        For example::

            >>> url = URL.from_text(u'http://localhost/a/b?x=y')
            >>> child_url = url.child(u"c", u"d")
            >>> child_url.to_text()
            u'http://localhost/a/b/c/d?x=y'

        Args:
           segments (unicode): Additional parts to be joined and added to
              the path, like :func:`os.path.join`. Special characters
              in segments will be percent encoded.

        Returns:
           URL: A copy of the current URL with the extra path segments.

        """
        if not segments:
            return self

        segments = [_textcheck('path segment', s) for s in segments]
        new_segs = _encode_path_parts(segments, joined=False, maximal=False)
        new_path = self.path[:-1 if (self.path and self.path[-1] == u'')
                             else None] + new_segs
        return self.replace(path=new_path)

    def sibling(self, segment):
        """Make a new :class:`URL` with a single path segment that is a
        sibling of this URL path.

        Args:
           segment (unicode): A single path segment.

        Returns:
           URL: A copy of the current URL with the last path segment
              replaced by *segment*. Special characters such as
              ``/?#`` will be percent encoded.

        """
        _textcheck('path segment', segment)
        new_path = self.path[:-1] + (_encode_path_part(segment),)
        return self.replace(path=new_path)

    def click(self, href=u''):
        """Resolve the given URL relative to this URL.

        The resulting URI should match what a web browser would
        generate if you visited the current URL and clicked on *href*.

           >>> url = URL.from_text(u'http://blog.hatnote.com/')
           >>> url.click(u'/post/155074058790').to_text()
           u'http://blog.hatnote.com/post/155074058790'
           >>> url = URL.from_text(u'http://localhost/a/b/c/')
           >>> url.click(u'../d/./e').to_text()
           u'http://localhost/a/b/d/e'

        Args:
            href (unicode): A string representing a clicked URL.

        Return:
            URL: A copy of the current URL with navigation logic applied.

        For more information, see `RFC 3986 section 5`_.

        .. _RFC 3986 section 5: https://tools.ietf.org/html/rfc3986#section-5
        """
        if href:
            if isinstance(href, URL):
                clicked = href
            else:
                # TODO: This error message is not completely accurate,
                # as URL objects are now also valid, but Twisted's
                # test suite (wrongly) relies on this exact message.
                _textcheck('relative URL', href)
                clicked = URL.from_text(href)
            if clicked.absolute:
                return clicked
        else:
            clicked = self

        query = clicked.query
        if clicked.scheme and not clicked.rooted:
            # Schemes with relative paths are not well-defined.  RFC 3986 calls
            # them a "loophole in prior specifications" that should be avoided,
            # or supported only for backwards compatibility.
            raise NotImplementedError('absolute URI with rootless path: %r'
                                      % (href,))
        else:
            if clicked.rooted:
                path = clicked.path
            elif clicked.path:
                path = self.path[:-1] + clicked.path
            else:
                path = self.path
                if not query:
                    query = self.query
        return self.replace(scheme=clicked.scheme or self.scheme,
                            host=clicked.host or self.host,
                            port=clicked.port or self.port,
                            path=_resolve_dot_segments(path),
                            query=query,
                            fragment=clicked.fragment)

    def to_uri(self):
        u"""Make a new :class:`URL` instance with all non-ASCII characters
        appropriately percent-encoded. This is useful to do in preparation
        for sending a :class:`URL` over a network protocol.

        For example::

            >>> URL.from_text(u'https://ايران.com/foo⇧bar/').to_uri()
            URL.from_text(u'https://xn--mgba3a4fra.com/foo%E2%87%A7bar/')

        Returns:
            URL: A new instance with its path segments, query parameters, and
            hostname encoded, so that they are all in the standard
            US-ASCII range.
        """
        new_userinfo = u':'.join([_encode_userinfo_part(p) for p in
                                  self.userinfo.split(':', 1)])
        new_path = _encode_path_parts(self.path, has_scheme=bool(self.scheme),
                                      rooted=False, joined=False, maximal=True)
        new_host = self.host if not self.host else idna_encode(self.host, uts46=True).decode("ascii")
        return self.replace(
            userinfo=new_userinfo,
            host=new_host,
            path=new_path,
            query=tuple([(_encode_query_key(k, maximal=True),
                          _encode_query_value(v, maximal=True)
                          if v is not None else None)
                         for k, v in self.query]),
            fragment=_encode_fragment_part(self.fragment, maximal=True)
        )

    def to_iri(self):
        u"""Make a new :class:`URL` instance with all but a few reserved
        characters decoded into human-readable format.

        Percent-encoded Unicode and IDNA-encoded hostnames are
        decoded, like so::

            >>> url = URL.from_text(u'https://xn--mgba3a4fra.example.com/foo%E2%87%A7bar/')
            >>> print(url.to_iri().to_text())
            https://ايران.example.com/foo⇧bar/

        .. note::

            As a general Python issue, "narrow" (UCS-2) builds of
            Python may not be able to fully decode certain URLs, and
            the in those cases, this method will return a best-effort,
            partially-decoded, URL which is still valid. This issue
            does not affect any Python builds 3.4+.

        Returns:
            URL: A new instance with its path segments, query parameters, and
            hostname decoded for display purposes.
        """
        new_userinfo = u':'.join([_decode_userinfo_part(p) for p in
                                  self.userinfo.split(':', 1)])
        host_text = _decode_host(self.host)

        return self.replace(userinfo=new_userinfo,
                            host=host_text,
                            path=[_decode_path_part(segment)
                                  for segment in self.path],
                            query=[(_decode_query_key(k),
                                    _decode_query_value(v)
                                    if v is not None else None)
                                   for k, v in self.query],
                            fragment=_decode_fragment_part(self.fragment))

    def to_text(self, with_password=False):
        """Render this URL to its textual representation.

        By default, the URL text will *not* include a password, if one
        is set. RFC 3986 considers using URLs to represent such
        sensitive information as deprecated. Quoting from RFC 3986,
        `section 3.2.1`:

            "Applications should not render as clear text any data after the
            first colon (":") character found within a userinfo subcomponent
            unless the data after the colon is the empty string (indicating no
            password)."

        Args:
            with_password (bool): Whether or not to include the
               password in the URL text. Defaults to False.

        Returns:
            str: The serialized textual representation of this URL,
            such as ``u"http://example.com/some/path?some=query"``.

        The natural counterpart to :class:`URL.from_text()`.

        .. _section 3.2.1: https://tools.ietf.org/html/rfc3986#section-3.2.1
        """
        scheme = self.scheme
        authority = self.authority(with_password)
        path = _encode_path_parts(self.path,
                                  rooted=self.rooted,
                                  has_scheme=bool(scheme),
                                  has_authority=bool(authority),
                                  maximal=False)
        query_parts = []
        for k, v in self.query:
            if v is None:
                query_parts.append(_encode_query_key(k, maximal=False))
            else:
                query_parts.append(u'='.join((_encode_query_key(k, maximal=False),
                                              _encode_query_value(v, maximal=False))))
        query_string = u'&'.join(query_parts)

        fragment = self.fragment

        parts = []
        _add = parts.append
        if scheme:
            _add(scheme)
            _add(':')
        if authority:
            _add('//')
            _add(authority)
        elif (scheme and path[:2] != '//' and self.uses_netloc):
            _add('//')
        if path:
            if scheme and authority and path[:1] != '/':
                _add('/')  # relpaths with abs authorities auto get '/'
            _add(path)
        if query_string:
            _add('?')
            _add(query_string)
        if fragment:
            _add('#')
            _add(fragment)
        return u''.join(parts)

    def __repr__(self):
        """Convert this URL to an representation that shows all of its
        constituent parts, as well as being a valid argument to
        :func:`eval`.
        """
        return '%s.from_text(%r)' % (self.__class__.__name__, self.to_text())

    def _to_bytes(self):
        """
        Allows for direct usage of URL objects with libraries like
        requests, which automatically stringify URL parameters. See
        issue #49.
        """
        return self.to_uri().to_text().encode('ascii')

    if PY2:
        __str__ = _to_bytes
        __unicode__ = to_text
    else:
        __bytes__ = _to_bytes
        __str__ = to_text

    # # Begin Twisted Compat Code
    asURI = to_uri
    asIRI = to_iri

    @classmethod
    def fromText(cls, s):
        return cls.from_text(s)

    def asText(self, includeSecrets=False):
        return self.to_text(with_password=includeSecrets)

    def __dir__(self):
        try:
            ret = object.__dir__(self)
        except AttributeError:
            # object.__dir__ == AttributeError  # pdw for py2
            ret = dir(self.__class__) + list(self.__dict__.keys())
        ret = sorted(set(ret) - set(['fromText', 'asURI', 'asIRI', 'asText']))
        return ret

    # # End Twisted Compat Code

    def add(self, name, value=None):
        """Make a new :class:`URL` instance with a given query argument,
        *name*, added to it with the value *value*, like so::

            >>> URL.from_text(u'https://example.com/?x=y').add(u'x')
            URL.from_text(u'https://example.com/?x=y&x')
            >>> URL.from_text(u'https://example.com/?x=y').add(u'x', u'z')
            URL.from_text(u'https://example.com/?x=y&x=z')

        Args:
            name (unicode): The name of the query parameter to add. The
                part before the ``=``.
            value (unicode): The value of the query parameter to add. The
                part after the ``=``. Defaults to ``None``, meaning no
                value.

        Returns:
            URL: A new :class:`URL` instance with the parameter added.
        """
        return self.replace(query=self.query + ((name, value),))

    def set(self, name, value=None):
        """Make a new :class:`URL` instance with the query parameter *name*
        set to *value*. All existing occurences, if any are replaced
        by the single name-value pair.

            >>> URL.from_text(u'https://example.com/?x=y').set(u'x')
            URL.from_text(u'https://example.com/?x')
            >>> URL.from_text(u'https://example.com/?x=y').set(u'x', u'z')
            URL.from_text(u'https://example.com/?x=z')

        Args:
            name (unicode): The name of the query parameter to set. The
                part before the ``=``.
            value (unicode): The value of the query parameter to set. The
                part after the ``=``. Defaults to ``None``, meaning no
                value.

        Returns:
            URL: A new :class:`URL` instance with the parameter set.
        """
        # Preserve the original position of the query key in the list
        q = [(k, v) for (k, v) in self.query if k != name]
        idx = next((i for (i, (k, v)) in enumerate(self.query)
                    if k == name), -1)
        q[idx:idx] = [(name, value)]
        return self.replace(query=q)

    def get(self, name):
        """Get a list of values for the given query parameter, *name*::

            >>> url = URL.from_text(u'?x=1&x=2')
            >>> url.get('x')
            [u'1', u'2']
            >>> url.get('y')
            []

        If the given *name* is not set, an empty list is returned. A
        list is always returned, and this method raises no exceptions.

        Args:
            name (unicode): The name of the query parameter to get.

        Returns:
            list: A list of all the values associated with the key, in
                string form.

        """
        return [value for (key, value) in self.query if name == key]

    def remove(self, name, value=_UNSET, limit=None):
        """Make a new :class:`URL` instance with occurrences of the query
        parameter *name* removed, or, if *value* is set, parameters
        matching *name* and *value*. No exception is raised if the
        parameter is not already set.

        Args:
            name (unicode): The name of the query parameter to remove.
            value (unicode): Optional value to additionally filter
               on. Setting this removes query parameters which match
               both name and value.
            limit (int): Optional maximum number of parameters to remove.

        Returns:
            URL: A new :class:`URL` instance with the parameter removed.
        """
        if limit is None:
            if value is _UNSET:
                nq = [(k, v) for (k, v) in self.query if k != name]
            else:
                nq = [(k, v) for (k, v) in self.query if not (k == name and v == value)]
        else:
            nq, removed_count = [], 0

            for k, v in self.query:
                if k == name and (value is _UNSET or v == value) and removed_count < limit:
                    removed_count += 1  # drop it
                else:
                    nq.append((k, v))  # keep it

        return self.replace(query=nq)


EncodedURL = URL  # An alias better describing what the URL really is


class DecodedURL(object):
    """DecodedURL is a type meant to act as a higher-level interface to
    the URL. It is the `unicode` to URL's `bytes`. `DecodedURL` has
    almost exactly the same API as `URL`, but everything going in and
    out is in its maximally decoded state. All percent decoding is
    handled automatically.

    Where applicable, a UTF-8 encoding is presumed. Be advised that
    some interactions can raise :exc:`UnicodeEncodeErrors` and
    :exc:`UnicodeDecodeErrors`, just like when working with
    bytestrings. Examples of such interactions include handling query
    strings encoding binary data, and paths containing segments with
    special characters encoded with codecs other than UTF-8.

    Args:
       url (URL): A :class:`URL` object to wrap.
       lazy (bool): Set to True to avoid pre-decode all parts of the
           URL to check for validity. Defaults to False.

    """
    def __init__(self, url, lazy=False):
        self._url = url
        if not lazy:
            # cache the following, while triggering any decoding
            # issues with decodable fields
            self.host, self.userinfo, self.path, self.query, self.fragment
        return

    @classmethod
    def from_text(cls, text, lazy=False):
        """\
        Make a `DecodedURL` instance from any text string containing a URL.

        Args:
          text (unicode): Text containing the URL
          lazy (bool): Whether to pre-decode all parts of the URL to
              check for validity. Defaults to True.
        """
        _url = URL.from_text(text)
        return cls(_url, lazy=lazy)

    @property
    def encoded_url(self):
        """Access the underlying :class:`URL` object, which has any special
        characters encoded.
        """
        return self._url

    def to_text(self, *a, **kw):
        "Passthrough to :meth:`~hyperlink.URL.to_text()`"
        return self._url.to_text(*a, **kw)

    def to_uri(self, *a, **kw):
        "Passthrough to :meth:`~hyperlink.URL.to_uri()`"
        return self._url.to_uri(*a, **kw)

    def to_iri(self, *a, **kw):
        "Passthrough to :meth:`~hyperlink.URL.to_iri()`"
        return self._url.to_iri(*a, **kw)

    def click(self, href=u''):
        "Return a new DecodedURL wrapping the result of :meth:`~hyperlink.URL.click()`"
        if isinstance(href, DecodedURL):
            href = href._url
        return self.__class__(self._url.click(href=href))

    def sibling(self, segment):
        """Automatically encode any reserved characters in *segment* and
        return a new `DecodedURL` wrapping the result of
        :meth:`~hyperlink.URL.sibling()`
        """
        return self.__class__(self._url.sibling(_encode_reserved(segment)))

    def child(self, *segments):
        """Automatically encode any reserved characters in *segments* and
        return a new `DecodedURL` wrapping the result of
        :meth:`~hyperlink.URL.child()`.
        """
        if not segments:
            return self
        new_segs = [_encode_reserved(s) for s in segments]
        return self.__class__(self._url.child(*new_segs))

    def normalize(self, *a, **kw):
        "Return a new `DecodedURL` wrapping the result of :meth:`~hyperlink.URL.normalize()`"
        return self.__class__(self._url.normalize(*a, **kw))

    @property
    def absolute(self):
        return self._url.absolute

    @property
    def scheme(self):
        return self._url.scheme

    @property
    def host(self):
        return _decode_host(self._url.host)

    @property
    def port(self):
        return self._url.port

    @property
    def rooted(self):
        return self._url.rooted

    @property
    def path(self):
        try:
            return self._path
        except AttributeError:
            pass
        self._path = tuple([_percent_decode(p, raise_subencoding_exc=True)
                            for p in self._url.path])
        return self._path

    @property
    def query(self):
        try:
            return self._query
        except AttributeError:
            pass
        _q = [tuple(_percent_decode(x, raise_subencoding_exc=True)
                    if x is not None else None
                    for x in (k, v))
              for k, v in self._url.query]
        self._query = tuple(_q)
        return self._query

    @property
    def fragment(self):
        try:
            return self._fragment
        except AttributeError:
            pass
        frag = self._url.fragment
        self._fragment = _percent_decode(frag, raise_subencoding_exc=True)
        return self._fragment

    @property
    def userinfo(self):
        try:
            return self._userinfo
        except AttributeError:
            pass
        self._userinfo = tuple([_percent_decode(p, raise_subencoding_exc=True)
                                for p in self._url.userinfo.split(':', 1)])
        return self._userinfo

    @property
    def user(self):
        return self.userinfo[0]

    @property
    def uses_netloc(self):
        return self._url.uses_netloc

    def replace(self, scheme=_UNSET, host=_UNSET, path=_UNSET, query=_UNSET,
                fragment=_UNSET, port=_UNSET, rooted=_UNSET, userinfo=_UNSET,
                uses_netloc=_UNSET):
        """While the signature is the same, this `replace()` differs a little
        from URL.replace. For instance, it accepts userinfo as a
        tuple, not as a string, handling the case of having a username
        containing a `:`. As with the rest of the methods on
        DecodedURL, if you pass a reserved character, it will be
        automatically encoded instead of an error being raised.

        """
        if path is not _UNSET:
            path = [_encode_reserved(p) for p in path]
        if query is not _UNSET:
            query = [[_encode_reserved(x)
                      if x is not None else None
                      for x in (k, v)]
                     for k, v in iter_pairs(query)]
        if userinfo is not _UNSET:
            if len(userinfo) > 2:
                raise ValueError('userinfo expected sequence of ["user"] or'
                                 ' ["user", "password"], got %r' % userinfo)
            userinfo = u':'.join([_encode_reserved(p) for p in userinfo])
        new_url = self._url.replace(scheme=scheme,
                                    host=host,
                                    path=path,
                                    query=query,
                                    fragment=fragment,
                                    port=port,
                                    rooted=rooted,
                                    userinfo=userinfo,
                                    uses_netloc=uses_netloc)
        return self.__class__(url=new_url)

    def get(self, name):
        "Get the value of all query parameters whose name matches *name*"
        return [v for (k, v) in self.query if name == k]

    def add(self, name, value=None):
        "Return a new DecodedURL with the query parameter *name* and *value* added."
        return self.replace(query=self.query + ((name, value),))

    def set(self, name, value=None):
        "Return a new DecodedURL with query parameter *name* set to *value*"
        query = self.query
        q = [(k, v) for (k, v) in query if k != name]
        idx = next((i for (i, (k, v)) in enumerate(query) if k == name), -1)
        q[idx:idx] = [(name, value)]
        return self.replace(query=q)

    def remove(self, name, value=_UNSET, limit=None):
        """Return a new DecodedURL with query parameter *name* removed.

        Optionally also filter for *value*, as well as cap the number
        of parameters removed with *limit*.
        """
        if limit is None:
            if value is _UNSET:
                nq = [(k, v) for (k, v) in self.query if k != name]
            else:
                nq = [(k, v) for (k, v) in self.query if not (k == name and v == value)]
        else:
            nq, removed_count = [], 0
            for k, v in self.query:
                if k == name and (value is _UNSET or v == value) and removed_count < limit:
                    removed_count += 1  # drop it
                else:
                    nq.append((k, v))  # keep it

        return self.replace(query=nq)

    def __repr__(self):
        cn = self.__class__.__name__
        return '%s(url=%r)' % (cn, self._url)

    def __str__(self):
        # TODO: the underlying URL's __str__ needs to change to make
        # this work as the URL, see #55
        return str(self._url)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.normalize().to_uri() == other.normalize().to_uri()

    def __ne__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.__class__, self.scheme, self.userinfo, self.host,
                     self.path, self.query, self.fragment, self.port,
                     self.rooted, self.uses_netloc))

    # # Begin Twisted Compat Code
    asURI = to_uri
    asIRI = to_iri

    @classmethod
    def fromText(cls, s, lazy=False):
        return cls.from_text(s, lazy=lazy)

    def asText(self, includeSecrets=False):
        return self.to_text(with_password=includeSecrets)

    def __dir__(self):
        try:
            ret = object.__dir__(self)
        except AttributeError:
            # object.__dir__ == AttributeError  # pdw for py2
            ret = dir(self.__class__) + list(self.__dict__.keys())
        ret = sorted(set(ret) - set(['fromText', 'asURI', 'asIRI', 'asText']))
        return ret

    # # End Twisted Compat Code


def parse(url, decoded=True, lazy=False):
    """Automatically turn text into a structured URL object.

    Args:

       decoded (bool): Whether or not to return a :class:`DecodedURL`,
          which automatically handles all
          encoding/decoding/quoting/unquoting for all the various
          accessors of parts of the URL, or an :class:`EncodedURL`,
          which has the same API, but requires handling of special
          characters for different parts of the URL.

       lazy (bool): In the case of `decoded=True`, this controls
          whether the URL is decoded immediately or as accessed. The
          default, `lazy=False`, checks all encoded parts of the URL
          for decodability.
    """
    enc_url = EncodedURL.from_text(url)
    if not decoded:
        return enc_url
    dec_url = DecodedURL(enc_url, lazy=lazy)
    return dec_url
