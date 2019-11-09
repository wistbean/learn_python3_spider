from hamcrest.core.base_matcher import BaseMatcher

__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"


class IsAnything(BaseMatcher):

    def __init__(self, description):
        self.description = description
        if not description:
            self.description = 'ANYTHING'

    def _matches(self, item):
        return True

    def describe_to(self, description):
        description.append_text(self.description)


def anything(description=None):
    """Matches anything.

    :param description: Optional string used to describe this matcher.

    This matcher always evaluates to ``True``. Specify this in composite
    matchers when the value of a particular element is unimportant.

    """
    return IsAnything(description)
