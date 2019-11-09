from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.core.anyof import any_of
from hamcrest.core.helpers.hasmethod import hasmethod
from hamcrest.core.helpers.wrap_matcher import wrap_matcher

__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"


class IsSequenceOnlyContaining(BaseMatcher):

    def __init__(self, matcher):
        self.matcher = matcher

    def _matches(self, sequence):
        try:
            sequence = list(sequence)
            if len(sequence) == 0:
                return False
            for item in sequence:
                if not self.matcher.matches(item):
                    return False
            return True
        except TypeError:
            return False

    def describe_to(self, description):
        description.append_text('a sequence containing items matching ')    \
                    .append_description_of(self.matcher)


def only_contains(*items):
    """Matches if each element of sequence satisfies any of the given matchers.

    :param match1,...: A comma-separated list of matchers.

    This matcher iterates the evaluated sequence, confirming whether each
    element satisfies any of the given matchers.

    Example::

        only_contains(less_than(4))

    will match ``[3,1,2]``.

    Any argument that is not a matcher is implicitly wrapped in an
    :py:func:`~hamcrest.core.core.isequal.equal_to` matcher to check for
    equality.

    """
    matchers = []
    for item in items:
        matchers.append(wrap_matcher(item))
    return IsSequenceOnlyContaining(any_of(*matchers))
