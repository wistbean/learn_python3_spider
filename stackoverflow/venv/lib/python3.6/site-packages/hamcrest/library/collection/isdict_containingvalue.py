from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.helpers.hasmethod import hasmethod
from hamcrest.core.helpers.wrap_matcher import wrap_matcher

__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"


class IsDictContainingValue(BaseMatcher):

    def __init__(self, value_matcher):
        self.value_matcher = value_matcher

    def _matches(self, dictionary):
        if hasmethod(dictionary, 'values'):
            for value in dictionary.values():
                if self.value_matcher.matches(value):
                    return True
        return False

    def describe_to(self, description):
        description.append_text('a dictionary containing value ')   \
                    .append_description_of(self.value_matcher)


def has_value(value):
    """Matches if dictionary contains an entry whose value satisfies a given
    matcher.

    :param value_match: The matcher to satisfy for the value, or an expected
        value for :py:func:`~hamcrest.core.core.isequal.equal_to` matching.

    This matcher iterates the evaluated dictionary, searching for any key-value
    entry whose value satisfies the given matcher. If a matching entry is
    found, ``has_value`` is satisfied.

    Any argument that is not a matcher is implicitly wrapped in an
    :py:func:`~hamcrest.core.core.isequal.equal_to` matcher to check for
    equality.

    Examples::

        has_value(equal_to('bar'))
        has_value('bar')

    """
    return IsDictContainingValue(wrap_matcher(value))
