__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"

from hamcrest.library.text.substringmatcher import SubstringMatcher
from hamcrest.core.helpers.hasmethod import hasmethod


class StringStartsWith(SubstringMatcher):

    def __init__(self, substring):
        super(StringStartsWith, self).__init__(substring)

    def _matches(self, item):
        if not hasmethod(item, 'startswith'):
            return False
        return item.startswith(self.substring)

    def relationship(self):
        return 'starting with'


def starts_with(substring):
    """Matches if object is a string starting with a given string.

    :param string: The string to search for.

    This matcher first checks whether the evaluated object is a string. If so,
    it checks if ``string`` matches the beginning characters of the evaluated
    object.

    Example::

        starts_with("foo")

    will match "foobar".

    """
    return StringStartsWith(substring)
