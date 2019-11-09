from __future__ import absolute_import
__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"

from hamcrest.core.base_matcher import BaseMatcher, Matcher
from hamcrest.core.helpers.wrap_matcher import wrap_matcher, is_matchable_type
from .isequal import equal_to
from .isinstanceof import instance_of


class IsNot(BaseMatcher):

    def __init__(self, matcher):
        self.matcher = matcher

    def _matches(self, item):
        return not self.matcher.matches(item)

    def describe_to(self, description):
        description.append_text('not ').append_description_of(self.matcher)


def wrap_value_or_type(x):
    if is_matchable_type(x):
        return instance_of(x)
    else:
        return wrap_matcher(x)


def is_not(match):
    """Inverts the given matcher to its logical negation.

    :param match: The matcher to negate.

    This matcher compares the evaluated object to the negation of the given
    matcher. If the ``match`` argument is not a matcher, it is implicitly
    wrapped in an :py:func:`~hamcrest.core.core.isequal.equal_to` matcher to
    check for equality, and thus matches for inequality.

    Examples::

        assert_that(cheese, is_not(equal_to(smelly)))
        assert_that(cheese, is_not(smelly))

    """
    return IsNot(wrap_value_or_type(match))

def not_(match):
    """Alias of :py:func:`is_not` for better readability of negations.

    Examples::

        assert_that(alist, not_(has_item(item)))

    """
    return is_not(match)
