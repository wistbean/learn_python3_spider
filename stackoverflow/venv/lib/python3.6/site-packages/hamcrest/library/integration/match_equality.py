from hamcrest.core.string_description import tostring
from hamcrest.core.helpers.wrap_matcher import wrap_matcher

__author__ = "Chris Rose"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"
__unittest = True


class EqualityWrapper(object):

    def __init__(self, matcher):
        self.matcher = matcher

    def __eq__(self, object):
        return self.matcher.matches(object)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return tostring(self.matcher)


def match_equality(matcher):
    """Wraps a matcher to define equality in terms of satisfying the matcher.

    ``match_equality`` allows Hamcrest matchers to be used in libraries that
    are not Hamcrest-aware. They might use the equality operator::

        assert match_equality(matcher) == object

    Or they might provide a method that uses equality for its test::

        library.method_that_tests_eq(match_equality(matcher))

    One concrete example is integrating with the ``assert_called_with`` methods
    in Michael Foord's `mock <http://www.voidspace.org.uk/python/mock/>`_
    library.

    """
    return EqualityWrapper(wrap_matcher(matcher))
