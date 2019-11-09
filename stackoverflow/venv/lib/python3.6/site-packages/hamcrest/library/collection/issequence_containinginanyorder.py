from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.helpers.hasmethod import hasmethod
from hamcrest.core.helpers.wrap_matcher import wrap_matcher

__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"


class MatchInAnyOrder(object):
    def __init__(self, matchers, mismatch_description):
        self.matchers = matchers[:]
        self.mismatch_description = mismatch_description

    def matches(self, item):
        return self.isnotsurplus(item) and self.ismatched(item)

    def isfinished(self, sequence):
        if not self.matchers:
            return True
        if self.mismatch_description:
            self.mismatch_description.append_text('no item matches: ')      \
                                .append_list('', ', ', '', self.matchers)   \
                                .append_text(' in ')                        \
                                .append_list('[', ', ', ']', sequence)
        return False

    def isnotsurplus(self, item):
        if not self.matchers:
            if self.mismatch_description:
                self.mismatch_description.append_text('not matched: ')  \
                                         .append_description_of(item)
            return False
        return True

    def ismatched(self, item):
        for index, matcher in enumerate(self.matchers):
            if matcher.matches(item):
                del self.matchers[index]
                return True

        if self.mismatch_description:
            self.mismatch_description.append_text('not matched: ')  \
                                     .append_description_of(item)
        return False


class IsSequenceContainingInAnyOrder(BaseMatcher):

    def __init__(self, matchers):
        self.matchers = matchers

    def matches(self, sequence, mismatch_description=None):
        try:
            sequence = list(sequence)
            matchsequence = MatchInAnyOrder(self.matchers, mismatch_description)
            for item in sequence:
                if not matchsequence.matches(item):
                    return False
            return matchsequence.isfinished(sequence)
        except TypeError:
            if mismatch_description:
                super(IsSequenceContainingInAnyOrder, self)             \
                    .describe_mismatch(sequence, mismatch_description)
            return False

    def describe_mismatch(self, item, mismatch_description):
        self.matches(item, mismatch_description)

    def describe_to(self, description):
        description.append_text('a sequence over ')             \
                   .append_list('[', ', ', ']', self.matchers)  \
                   .append_text(' in any order')


def contains_inanyorder(*items):
    """Matches if sequences's elements, in any order, satisfy a given list of
    matchers.

    :param match1,...: A comma-separated list of matchers.

    This matcher iterates the evaluated sequence, seeing if each element
    satisfies any of the given matchers. The matchers are tried from left to
    right, and when a satisfied matcher is found, it is no longer a candidate
    for the remaining elements. If a one-to-one correspondence is established
    between elements and matchers, ``contains_inanyorder`` is satisfied.

    Any argument that is not a matcher is implicitly wrapped in an
    :py:func:`~hamcrest.core.core.isequal.equal_to` matcher to check for
    equality.

    """

    matchers = []
    for item in items:
        matchers.append(wrap_matcher(item))
    return IsSequenceContainingInAnyOrder(matchers)
