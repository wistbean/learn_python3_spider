import six

from hamcrest.core.base_matcher import Matcher
from hamcrest.core.core.isequal import equal_to

__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"

import types

def wrap_matcher(x):
    """Wraps argument in a matcher, if necessary.

    :returns: the argument as-is if it is already a matcher, otherwise wrapped
        in an :py:func:`~hamcrest.core.core.isequal.equal_to` matcher.

    """
    if isinstance(x, Matcher):
        return x
    else:
        return equal_to(x)

def is_matchable_type(expected_type):
    if isinstance(expected_type, type):
        return True

    if isinstance(expected_type, six.class_types):
        return True

    if isinstance(expected_type, tuple) and \
       expected_type and \
       all(map(is_matchable_type, expected_type)):
        return True

    return False
