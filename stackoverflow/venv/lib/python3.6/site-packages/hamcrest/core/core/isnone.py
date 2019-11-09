from __future__ import absolute_import
__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"

from hamcrest.core.base_matcher import BaseMatcher
from .isnot import is_not


class IsNone(BaseMatcher):

    def _matches(self, item):
        return item is None

    def describe_to(self, description):
        description.append_text('None')


def none():
    """Matches if object is ``None``."""
    return IsNone()


def not_none():
    """Matches if object is not ``None``."""
    return is_not(none())
