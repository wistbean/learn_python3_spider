from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.helpers.wrap_matcher import wrap_matcher

__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"


class AllOf(BaseMatcher):

    def __init__(self, *matchers):
        self.matchers = matchers

    def matches(self, item, mismatch_description=None):
        for matcher in self.matchers:
            if not matcher.matches(item):
                if mismatch_description:
                    mismatch_description.append_description_of(matcher) \
                                        .append_text(' ')
                    matcher.describe_mismatch(item, mismatch_description)
                return False
        return True

    def describe_mismatch(self, item, mismatch_description):
        self.matches(item, mismatch_description)

    def describe_to(self, description):
        description.append_list('(', ' and ', ')', self.matchers)


def all_of(*items):
    """Matches if all of the given matchers evaluate to ``True``.

    :param matcher1,...:  A comma-separated list of matchers.

    The matchers are evaluated from left to right using short-circuit
    evaluation, so evaluation stops as soon as a matcher returns ``False``.

    Any argument that is not a matcher is implicitly wrapped in an
    :py:func:`~hamcrest.core.core.isequal.equal_to` matcher to check for
    equality.

    """
    return AllOf(*[wrap_matcher(item) for item in items])
