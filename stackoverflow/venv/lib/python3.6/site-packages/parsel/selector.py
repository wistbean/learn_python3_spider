"""
XPath selectors based on lxml
"""

import sys

import six
from lxml import etree, html

from .utils import flatten, iflatten, extract_regex, shorten
from .csstranslator import HTMLTranslator, GenericTranslator


class SafeXMLParser(etree.XMLParser):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('resolve_entities', False)
        super(SafeXMLParser, self).__init__(*args, **kwargs)

_ctgroup = {
    'html': {'_parser': html.HTMLParser,
             '_csstranslator': HTMLTranslator(),
             '_tostring_method': 'html'},
    'xml': {'_parser': SafeXMLParser,
            '_csstranslator': GenericTranslator(),
            '_tostring_method': 'xml'},
}


def _st(st):
    if st is None:
        return 'html'
    elif st in _ctgroup:
        return st
    else:
        raise ValueError('Invalid type: %s' % st)


def create_root_node(text, parser_cls, base_url=None):
    """Create root node for text using given parser class.
    """
    body = text.strip().replace('\x00', '').encode('utf8') or b'<html/>'
    parser = parser_cls(recover=True, encoding='utf8')
    root = etree.fromstring(body, parser=parser, base_url=base_url)
    if root is None:
        root = etree.fromstring(b'<html/>', parser=parser, base_url=base_url)
    return root


class SelectorList(list):
    """
    The :class:`SelectorList` class is a subclass of the builtin ``list``
    class, which provides a few additional methods.
    """

    # __getslice__ is deprecated but `list` builtin implements it only in Py2
    def __getslice__(self, i, j):
        o = super(SelectorList, self).__getslice__(i, j)
        return self.__class__(o)

    def __getitem__(self, pos):
        o = super(SelectorList, self).__getitem__(pos)
        return self.__class__(o) if isinstance(pos, slice) else o

    def __getstate__(self):
        raise TypeError("can't pickle SelectorList objects")

    def xpath(self, xpath, namespaces=None, **kwargs):
        """
        Call the ``.xpath()`` method for each element in this list and return
        their results flattened as another :class:`SelectorList`.

        ``query`` is the same argument as the one in :meth:`Selector.xpath`

        ``namespaces`` is an optional ``prefix: namespace-uri`` mapping (dict)
        for additional prefixes to those registered with ``register_namespace(prefix, uri)``.
        Contrary to ``register_namespace()``, these prefixes are not
        saved for future calls.

        Any additional named arguments can be used to pass values for XPath
        variables in the XPath expression, e.g.::

            selector.xpath('//a[href=$url]', url="http://www.example.com")
        """
        return self.__class__(flatten([x.xpath(xpath, namespaces=namespaces, **kwargs) for x in self]))

    def css(self, query):
        """
        Call the ``.css()`` method for each element in this list and return
        their results flattened as another :class:`SelectorList`.

        ``query`` is the same argument as the one in :meth:`Selector.css`
        """
        return self.__class__(flatten([x.css(query) for x in self]))

    def re(self, regex, replace_entities=True):
        """
        Call the ``.re()`` method for each element in this list and return
        their results flattened, as a list of unicode strings.

        By default, character entity references are replaced by their
        corresponding character (except for ``&amp;`` and ``&lt;``.
        Passing ``replace_entities`` as ``False`` switches off these
        replacements.
        """
        return flatten([x.re(regex, replace_entities=replace_entities) for x in self])

    def re_first(self, regex, default=None, replace_entities=True):
        """
        Call the ``.re()`` method for the first element in this list and
        return the result in an unicode string. If the list is empty or the
        regex doesn't match anything, return the default value (``None`` if
        the argument is not provided).

        By default, character entity references are replaced by their
        corresponding character (except for ``&amp;`` and ``&lt;``.
        Passing ``replace_entities`` as ``False`` switches off these
        replacements.
        """
        for el in iflatten(x.re(regex, replace_entities=replace_entities) for x in self):
            return el
        else:
            return default

    def getall(self):
        """
        Call the ``.get()`` method for each element is this list and return
        their results flattened, as a list of unicode strings.
        """
        return [x.get() for x in self]
    extract = getall

    def get(self, default=None):
        """
        Return the result of ``.get()`` for the first element in this list.
        If the list is empty, return the default value.
        """
        for x in self:
            return x.get()
        else:
            return default
    extract_first = get

    @property
    def attrib(self):
        """Return the attributes dictionary for the first element.
        If the list is empty, return an empty dict.
        """
        for x in self:
            return x.attrib
        else:
            return {}


class Selector(object):
    """
    :class:`Selector` allows you to select parts of an XML or HTML text using CSS
    or XPath expressions and extract data from it.

    ``text`` is a ``unicode`` object in Python 2 or a ``str`` object in Python 3

    ``type`` defines the selector type, it can be ``"html"``, ``"xml"`` or ``None`` (default).
    If ``type`` is ``None``, the selector defaults to ``"html"``.
    """

    __slots__ = ['text', 'namespaces', 'type', '_expr', 'root',
                 '__weakref__', '_parser', '_csstranslator', '_tostring_method']

    _default_type = None
    _default_namespaces = {
        "re": "http://exslt.org/regular-expressions",

        # supported in libxslt:
        # set:difference
        # set:has-same-node
        # set:intersection
        # set:leading
        # set:trailing
        "set": "http://exslt.org/sets"
    }
    _lxml_smart_strings = False
    selectorlist_cls = SelectorList

    def __init__(self, text=None, type=None, namespaces=None, root=None,
                 base_url=None, _expr=None):
        self.type = st = _st(type or self._default_type)
        self._parser = _ctgroup[st]['_parser']
        self._csstranslator = _ctgroup[st]['_csstranslator']
        self._tostring_method = _ctgroup[st]['_tostring_method']

        if text is not None:
            if not isinstance(text, six.text_type):
                raise TypeError("text argument should be of type %s" % six.text_type)
            root = self._get_root(text, base_url)
        elif root is None:
            raise ValueError("Selector needs either text or root argument")

        self.namespaces = dict(self._default_namespaces)
        if namespaces is not None:
            self.namespaces.update(namespaces)
        self.root = root
        self._expr = _expr

    def __getstate__(self):
        raise TypeError("can't pickle Selector objects")

    def _get_root(self, text, base_url=None):
        return create_root_node(text, self._parser, base_url=base_url)

    def xpath(self, query, namespaces=None, **kwargs):
        """
        Find nodes matching the xpath ``query`` and return the result as a
        :class:`SelectorList` instance with all elements flattened. List
        elements implement :class:`Selector` interface too.

        ``query`` is a string containing the XPATH query to apply.

        ``namespaces`` is an optional ``prefix: namespace-uri`` mapping (dict)
        for additional prefixes to those registered with ``register_namespace(prefix, uri)``.
        Contrary to ``register_namespace()``, these prefixes are not
        saved for future calls.

        Any additional named arguments can be used to pass values for XPath
        variables in the XPath expression, e.g.::

            selector.xpath('//a[href=$url]', url="http://www.example.com")
        """
        try:
            xpathev = self.root.xpath
        except AttributeError:
            return self.selectorlist_cls([])

        nsp = dict(self.namespaces)
        if namespaces is not None:
            nsp.update(namespaces)
        try:
            result = xpathev(query, namespaces=nsp,
                             smart_strings=self._lxml_smart_strings,
                             **kwargs)
        except etree.XPathError as exc:
            msg = u"XPath error: %s in %s" % (exc, query)
            msg = msg if six.PY3 else msg.encode('unicode_escape')
            six.reraise(ValueError, ValueError(msg), sys.exc_info()[2])

        if type(result) is not list:
            result = [result]

        result = [self.__class__(root=x, _expr=query,
                                 namespaces=self.namespaces,
                                 type=self.type)
                  for x in result]
        return self.selectorlist_cls(result)

    def css(self, query):
        """
        Apply the given CSS selector and return a :class:`SelectorList` instance.

        ``query`` is a string containing the CSS selector to apply.

        In the background, CSS queries are translated into XPath queries using
        `cssselect`_ library and run ``.xpath()`` method.
        
        .. _cssselect: https://pypi.python.org/pypi/cssselect/
        """
        return self.xpath(self._css2xpath(query))

    def _css2xpath(self, query):
        return self._csstranslator.css_to_xpath(query)

    def re(self, regex, replace_entities=True):
        """
        Apply the given regex and return a list of unicode strings with the
        matches.

        ``regex`` can be either a compiled regular expression or a string which
        will be compiled to a regular expression using ``re.compile(regex)``.

        By default, character entity references are replaced by their
        corresponding character (except for ``&amp;`` and ``&lt;``).
        Passing ``replace_entities`` as ``False`` switches off these
        replacements.
        """
        return extract_regex(regex, self.get(), replace_entities=replace_entities)

    def re_first(self, regex, default=None, replace_entities=True):
        """
        Apply the given regex and return the first unicode string which
        matches. If there is no match, return the default value (``None`` if
        the argument is not provided).

        By default, character entity references are replaced by their
        corresponding character (except for ``&amp;`` and ``&lt;``).
        Passing ``replace_entities`` as ``False`` switches off these
        replacements.
        """
        return next(iflatten(self.re(regex, replace_entities=replace_entities)), default)

    def get(self):
        """
        Serialize and return the matched nodes in a single unicode string.
        Percent encoded content is unquoted.
        """
        try:
            return etree.tostring(self.root,
                                  method=self._tostring_method,
                                  encoding='unicode',
                                  with_tail=False)
        except (AttributeError, TypeError):
            if self.root is True:
                return u'1'
            elif self.root is False:
                return u'0'
            else:
                return six.text_type(self.root)
    extract = get

    def getall(self):
        """
        Serialize and return the matched node in a 1-element list of unicode strings.
        """
        return [self.get()]

    def register_namespace(self, prefix, uri):
        """
        Register the given namespace to be used in this :class:`Selector`.
        Without registering namespaces you can't select or extract data from
        non-standard namespaces. See :ref:`selector-examples-xml`.
        """
        self.namespaces[prefix] = uri

    def remove_namespaces(self):
        """
        Remove all namespaces, allowing to traverse the document using
        namespace-less xpaths. See :ref:`removing-namespaces`.
        """
        for el in self.root.iter('*'):
            if el.tag.startswith('{'):
                el.tag = el.tag.split('}', 1)[1]
            # loop on element attributes also
            for an in el.attrib.keys():
                if an.startswith('{'):
                    el.attrib[an.split('}', 1)[1]] = el.attrib.pop(an)
        # remove namespace declarations
        etree.cleanup_namespaces(self.root)

    @property
    def attrib(self):
        """Return the attributes dictionary for underlying element.
        """
        return dict(self.root.attrib)

    def __bool__(self):
        """
        Return ``True`` if there is any real content selected or ``False``
        otherwise.  In other words, the boolean value of a :class:`Selector` is
        given by the contents it selects.
        """
        return bool(self.get())
    __nonzero__ = __bool__

    def __str__(self):
        data = repr(shorten(self.get(), width=40))
        return "<%s xpath=%r data=%s>" % (type(self).__name__, self._expr, data)
    __repr__ = __str__
