import six
from hamcrest.core.base_matcher import BaseMatcher
from math import fabs

__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"


def isnumeric(value):
    """Confirm that 'value' can be treated numerically; duck-test accordingly
    """
    if isinstance(value, (float, complex) + six.integer_types):
        return True

    try:
        _ = (fabs(value) + 0 - 0) * 1
        return True
    except ArithmeticError:
        return True
    except:
        return False
    return False


class IsCloseTo(BaseMatcher):

    def __init__(self, value, delta):
        if not isnumeric(value):
            raise TypeError('IsCloseTo value must be numeric')
        if not isnumeric(delta):
            raise TypeError('IsCloseTo delta must be numeric')

        self.value = value
        self.delta = delta

    def _matches(self, item):
        if not isnumeric(item):
            return False
        return fabs(item - self.value) <= self.delta

    def describe_mismatch(self, item, mismatch_description):
        if not isnumeric(item):
            super(IsCloseTo, self).describe_mismatch(item, mismatch_description)
        else:
            actual_delta = fabs(item - self.value)
            mismatch_description.append_description_of(item)            \
                                .append_text(' differed by ')           \
                                .append_description_of(actual_delta)

    def describe_to(self, description):
        description.append_text('a numeric value within ')  \
                   .append_description_of(self.delta)       \
                   .append_text(' of ')                     \
                   .append_description_of(self.value)


def close_to(value, delta):
    """Matches if object is a number close to a given value, within a given
    delta.

    :param value: The value to compare against as the expected value.
    :param delta: The maximum delta between the values for which the numbers
        are considered close.

    This matcher compares the evaluated object against ``value`` to see if the
    difference is within a positive ``delta``.

    Example::

        close_to(3.0, 0.25)

    """
    return IsCloseTo(value, delta)
