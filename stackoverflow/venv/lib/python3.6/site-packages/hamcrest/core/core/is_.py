from __future__ import absolute_import
__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"

from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.helpers.wrap_matcher import wrap_matcher, is_matchable_type
from .isinstanceof import instance_of


class Is(BaseMatcher):

    def __init__(self, matcher):
        self.matcher = matcher

    def matches(self, item, mismatch_description=None):
        return self.matcher.matches(item, mismatch_description)

    def describe_mismatch(self, item, mismatch_description):
        return self.matcher.describe_mismatch(item, mismatch_description)

    def describe_to(self, description):
        description.append_description_of(self.matcher)


def wrap_value_or_type(x):
    if is_matchable_type(x):
        return instance_of(x)
    else:
        return wrap_matcher(x)


def is_(x):
    """Decorates another matcher, or provides shortcuts to the frequently used
    ``is(equal_to(x))`` and ``is(instance_of(x))``.

    :param x: The matcher to satisfy,  or a type for
        :py:func:`~hamcrest.core.core.isinstanceof.instance_of` matching, or an
        expected value for :py:func:`~hamcrest.core.core.isequal.equal_to`
        matching.

    This matcher compares the evaluated object to the given matcher.

    .. note::

        PyHamcrest's ``is_`` matcher is unrelated to Python's ``is`` operator.
        The matcher for object identity is
        :py:func:`~hamcrest.core.core.issame.same_instance`.

    If the ``x`` argument is a matcher, its behavior is retained, but the test
    may be more expressive. For example::

        assert_that(value, less_than(5))
        assert_that(value, is_(less_than(5)))

    If the ``x`` argument is a type, it is wrapped in an
    :py:func:`~hamcrest.core.core.isinstanceof.instance_of` matcher. This makes
    the following statements equivalent::

        assert_that(cheese, instance_of(Cheddar))
        assert_that(cheese, is_(instance_of(Cheddar)))
        assert_that(cheese, is_(Cheddar))

    Otherwise, if the ``x`` argument is not a matcher, it is wrapped in an
    :py:func:`~hamcrest.core.core.isequal.equal_to` matcher. This makes the
    following statements equivalent::

        assert_that(cheese, equal_to(smelly))
        assert_that(cheese, is_(equal_to(smelly)))
        assert_that(cheese, is_(smelly))

    Choose the style that makes your expression most readable. This will vary
    depending on context.

    """
    return Is(wrap_value_or_type(x))
