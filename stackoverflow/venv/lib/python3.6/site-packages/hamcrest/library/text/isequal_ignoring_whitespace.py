__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"

from hamcrest.core.base_matcher import BaseMatcher

import six

def stripspace(string):
    result = ''
    last_was_space = True
    for character in string:
        if character.isspace():
            if not last_was_space:
                result += ' '
            last_was_space = True
        else:
            result += character
            last_was_space = False
    return result.strip()


class IsEqualIgnoringWhiteSpace(BaseMatcher):

    def __init__(self, string):
        if not isinstance(string, six.string_types):
            raise TypeError('IsEqualIgnoringWhiteSpace requires string')
        self.original_string = string
        self.stripped_string = stripspace(string)

    def _matches(self, item):
        if not isinstance(item, six.string_types):
            return False
        return self.stripped_string == stripspace(item)

    def describe_to(self, description):
        description.append_description_of(self.original_string)    \
                   .append_text(' ignoring whitespace')


def equal_to_ignoring_whitespace(string):
    """Matches if object is a string equal to a given string, ignoring
    differences in whitespace.

    :param string: The string to compare against as the expected value.

    This matcher first checks whether the evaluated object is a string. If so,
    it compares it with ``string``, ignoring differences in runs of whitespace.

    Example::

        equal_to_ignoring_whitespace("hello world")

    will match ``"hello   world"``.

    """
    return IsEqualIgnoringWhiteSpace(string)
