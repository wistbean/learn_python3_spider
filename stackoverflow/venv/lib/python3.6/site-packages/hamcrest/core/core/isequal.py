__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"

from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.matcher import Matcher


class IsEqual(BaseMatcher):

    def __init__(self, equals):
        self.object = equals

    def _matches(self, item):
        return item == self.object

    def describe_to(self, description):
        nested_matcher = isinstance(self.object, Matcher)
        if nested_matcher:
            description.append_text('<')
        description.append_description_of(self.object)
        if nested_matcher:
            description.append_text('>')


def equal_to(obj):
    """Matches if object is equal to a given object.

    :param obj: The object to compare against as the expected value.

    This matcher compares the evaluated object to ``obj`` for equality."""
    return IsEqual(obj)
