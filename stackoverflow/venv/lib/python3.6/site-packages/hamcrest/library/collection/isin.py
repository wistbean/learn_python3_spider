from hamcrest.core.base_matcher import BaseMatcher

__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"


class IsIn(BaseMatcher):

    def __init__(self, sequence):
        self.sequence = sequence

    def _matches(self, item):
        return item in self.sequence

    def describe_to(self, description):
        description.append_text('one of ')      \
                   .append_list('(', ', ', ')', self.sequence)


def is_in(sequence):
    """Matches if evaluated object is present in a given sequence.

    :param sequence: The sequence to search.

    This matcher invokes the ``in`` membership operator to determine if the
    evaluated object is a member of the sequence.

    """
    return IsIn(sequence)
