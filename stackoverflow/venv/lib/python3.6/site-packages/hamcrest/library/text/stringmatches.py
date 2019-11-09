__author__ = "Chris Rose"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"

import re

import six

from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.helpers.hasmethod import hasmethod

class StringMatchesPattern(BaseMatcher):

    def __init__(self, pattern):
        self.pattern = pattern

    def describe_to(self, description):
        description.append_text("a string matching '") \
                                   .append_text(self.pattern.pattern) \
                                   .append_text("'")

    def _matches(self, item):
        return self.pattern.search(item) is not None


def matches_regexp(pattern):
    """Matches if object is a string containing a match for a given regular
    expression.

    :param pattern: The regular expression to search for.

    This matcher first checks whether the evaluated object is a string. If so,
    it checks if the regular expression ``pattern`` matches anywhere within the
    evaluated object.

    """
    if isinstance(pattern, six.string_types):
        pattern = re.compile(pattern)

    return StringMatchesPattern(pattern)
