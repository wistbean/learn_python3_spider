__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"

from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.helpers.hasmethod import hasmethod
from hamcrest.core.helpers.wrap_matcher import wrap_matcher


class MatchingInOrder(object):
    def __init__(self, matchers, mismatch_description):
        self.matchers = matchers
        self.mismatch_description = mismatch_description
        self.next_match_index = 0

    def matches(self, item):
        return self.isnotsurplus(item) and self.ismatched(item)

    def isfinished(self):
        if self.next_match_index < len(self.matchers):
            if self.mismatch_description:
                self.mismatch_description.append_text('No item matched: ') \
                                 .append_description_of(self.matchers[self.next_match_index])
            return False
        return True

    def ismatched(self, item):
        matcher = self.matchers[self.next_match_index]
        if not matcher.matches(item):
            if self.mismatch_description:
                self.mismatch_description.append_text('item ' + str(self.next_match_index) + ': ')
                matcher.describe_mismatch(item, self.mismatch_description)
            return False
        self.next_match_index += 1
        return True

    def isnotsurplus(self, item):
        if len(self.matchers) <= self.next_match_index:
            if self.mismatch_description:
                self.mismatch_description.append_text('Not matched: ')  \
                                         .append_description_of(item)
            return False
        return True


class IsSequenceContainingInOrder(BaseMatcher):

    def __init__(self, matchers):
        self.matchers = matchers

    def matches(self, sequence, mismatch_description=None):
        try:
            matchsequence = MatchingInOrder(self.matchers, mismatch_description)
            for item in sequence:
                if not matchsequence.matches(item):
                    return False
            return matchsequence.isfinished()
        except TypeError:
            if mismatch_description:
                super(IsSequenceContainingInOrder, self)                \
                    .describe_mismatch(sequence, mismatch_description)
            return False

    def describe_mismatch(self, item, mismatch_description):
        self.matches(item, mismatch_description)

    def describe_to(self, description):
        description.append_text('a sequence containing ')   \
                   .append_list('[', ', ', ']', self.matchers)


def contains(*items):
    """Matches if sequence's elements satisfy a given list of matchers, in order.

    :param match1,...: A comma-separated list of matchers.

    This matcher iterates the evaluated sequence and a given list of matchers,
    seeing if each element satisfies its corresponding matcher.

    Any argument that is not a matcher is implicitly wrapped in an
    :py:func:`~hamcrest.core.core.isequal.equal_to` matcher to check for
    equality.

    """
    matchers = []
    for item in items:
        matchers.append(wrap_matcher(item))
    return IsSequenceContainingInOrder(matchers)
