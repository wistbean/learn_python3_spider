from __future__ import absolute_import
__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"

import warnings
import six

from hamcrest.core.description import Description
from hamcrest.core.selfdescribingvalue import SelfDescribingValue
from hamcrest.core.helpers.hasmethod import hasmethod

class BaseDescription(Description):
    """Base class for all :py:class:`~hamcrest.core.description.Description`
    implementations.

    """

    def append_text(self, text):
        self.append(text)
        return self

    def append_description_of(self, value):
        if hasmethod(value, 'describe_to'):
            value.describe_to(self)
        elif six.PY3 and isinstance(value, six.text_type):
            self.append(repr(value))
        elif six.PY2 and isinstance(value, six.binary_type):
            self.append_string_in_python_syntax(value)
        elif isinstance(value, six.text_type):
            self.append_string_in_python_syntax(value)
        else:
            description = str(value)
            if description[:1] == '<' and description[-1:] == '>':
                self.append(description)
            else:
                self.append('<')
                self.append(description)
                self.append('>')
        return self

    def append_value(self, value):
        warnings.warn('Call append_description_of instead of append_value',
                      DeprecationWarning)
        if isinstance(value, str):
            self.append_string_in_python_syntax(value)
        else:
            self.append('<')
            self.append(str(value))
            self.append('>')
        return self

    def append_value_list(self, start, separator, end, list):
        warnings.warn('Call append_list instead of append_value_list',
                      DeprecationWarning)
        return self.append_list(start, separator, end,
                                map(SelfDescribingValue, list))

    def append_list(self, start, separator, end, list):
        separate = False

        self.append(start)
        for item in list:
            if separate:
                self.append(separator)
            self.append_description_of(item)
            separate = True
        self.append(end)
        return self

    def append(self, string):
        """Append the string to the description."""
        raise NotImplementedError('append')

    def append_string_in_python_syntax(self, string):
        self.append("'")
        for ch in string:
            self.append(character_in_python_syntax(ch))
        self.append("'")


def character_in_python_syntax(ch):
    if ch == "'":
        return "\'"
    elif ch == '\n':
        return '\\n'
    elif ch == '\r':
        return '\\r'
    elif ch == '\t':
        return '\\t'
    else:
        return ch
