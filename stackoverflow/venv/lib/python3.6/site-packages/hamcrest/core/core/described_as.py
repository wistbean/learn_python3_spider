from hamcrest.core.base_matcher import BaseMatcher
import re

__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"


ARG_PATTERN = re.compile('%([0-9]+)')


class DescribedAs(BaseMatcher):

    def __init__(self, description_template, matcher, *values):
        self.template = description_template
        self.matcher = matcher
        self.values = values

    def matches(self, item, mismatch_description=None):
        return self.matcher.matches(item, mismatch_description)

    def describe_mismatch(self, item, mismatch_description):
        self.matcher.describe_mismatch(item, mismatch_description)

    def describe_to(self, description):
        text_start = 0
        for match in re.finditer(ARG_PATTERN, self.template):
            description.append_text(self.template[text_start:match.start()])
            arg_index = int(match.group()[1:])
            description.append_description_of(self.values[arg_index])
            text_start = match.end()

        if text_start < len(self.template):
            description.append_text(self.template[text_start:])


def described_as(description, matcher, *values):
    """Adds custom failure description to a given matcher.

    :param description: Overrides the matcher's description.
    :param matcher: The matcher to satisfy.
    :param value1,...: Optional comma-separated list of substitution values.

    The description may contain substitution placeholders %0, %1, etc. These
    will be replaced by any values that follow the matcher.

    """
    return DescribedAs(description, matcher, *values)
