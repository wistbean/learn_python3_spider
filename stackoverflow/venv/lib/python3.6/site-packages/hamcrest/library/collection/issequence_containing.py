__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"

from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.core.allof import all_of
from hamcrest.core.helpers.hasmethod import hasmethod
from hamcrest.core.helpers.wrap_matcher import wrap_matcher


class IsSequenceContaining(BaseMatcher):

    def __init__(self, element_matcher):
        self.element_matcher = element_matcher

    def _matches(self, sequence):
        try:
            for item in sequence:
                if self.element_matcher.matches(item):
                    return True
        except TypeError: # not a sequence
            return False

    def describe_to(self, description):
        description.append_text('a sequence containing ')           \
                    .append_description_of(self.element_matcher)


# It'd be great to make use of all_of, but we can't be sure we won't
# be seeing a one-time sequence here (like a generator); see issue #20
# Instead, we wrap it inside a class that will convert the sequence into
# a concrete list and then hand it off to the all_of matcher.
class IsSequenceContainingEvery(BaseMatcher):

    def __init__(self, *element_matchers):
        delegates = [has_item(e) for e in element_matchers]
        self.matcher = all_of(*delegates)

    def _matches(self, sequence):
        try:
            return self.matcher.matches(list(sequence))
        except TypeError:
            return False

    def describe_mismatch(self, item, mismatch_description):
        self.matcher.describe_mismatch(item, mismatch_description)

    def describe_to(self, description):
        self.matcher.describe_to(description)



def has_item(match):
    """Matches if any element of sequence satisfies a given matcher.

    :param match: The matcher to satisfy, or an expected value for
        :py:func:`~hamcrest.core.core.isequal.equal_to` matching.

    This matcher iterates the evaluated sequence, searching for any element
    that satisfies a given matcher. If a matching element is found,
    ``has_item`` is satisfied.

    If the ``match`` argument is not a matcher, it is implicitly wrapped in an
    :py:func:`~hamcrest.core.core.isequal.equal_to` matcher to check for
    equality.

    """
    return IsSequenceContaining(wrap_matcher(match))


def has_items(*items):
    """Matches if all of the given matchers are satisfied by any elements of
    the sequence.

    :param match1,...: A comma-separated list of matchers.

    This matcher iterates the given matchers, searching for any elements in the
    evaluated sequence that satisfy them. If each matcher is satisfied, then
    ``has_items`` is satisfied.

    Any argument that is not a matcher is implicitly wrapped in an
    :py:func:`~hamcrest.core.core.isequal.equal_to` matcher to check for
    equality.

    """
    matchers = []
    for item in items:
        matchers.append(wrap_matcher(item))
    return IsSequenceContainingEvery(*matchers)
