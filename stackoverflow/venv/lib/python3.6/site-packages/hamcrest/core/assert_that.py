from __future__ import absolute_import
from hamcrest.core.matcher import Matcher
from hamcrest.core.string_description import StringDescription

__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"
# unittest integration; hide these frames from tracebacks
__unittest = True
# py.test integration; hide these frames from tracebacks
__tracebackhide__ = True

def assert_that(arg1, arg2=None, arg3=''):
    """Asserts that actual value satisfies matcher. (Can also assert plain
    boolean condition.)

    :param actual: The object to evaluate as the actual value.
    :param matcher: The matcher to satisfy as the expected condition.
    :param reason: Optional explanation to include in failure description.

    ``assert_that`` passes the actual value to the matcher for evaluation. If
    the matcher is not satisfied, an exception is thrown describing the
    mismatch.

    ``assert_that`` is designed to integrate well with PyUnit and other unit
    testing frameworks. The exception raised for an unmet assertion is an
    :py:exc:`AssertionError`, which PyUnit reports as a test failure.

    With a different set of parameters, ``assert_that`` can also verify a
    boolean condition:

    .. function:: assert_that(assertion[, reason])

    :param assertion:  Boolean condition to verify.
    :param reason:  Optional explanation to include in failure description.

    This is equivalent to the :py:meth:`~unittest.TestCase.assertTrue` method
    of :py:class:`unittest.TestCase`, but offers greater flexibility in test
    writing by being a standalone function.

    """
    if isinstance(arg2, Matcher):
        _assert_match(actual=arg1, matcher=arg2, reason=arg3)
    else:
        _assert_bool(assertion=arg1, reason=arg2)


def _assert_match(actual, matcher, reason):
    if not matcher.matches(actual):
        description = StringDescription()
        description.append_text(reason)             \
                   .append_text('\nExpected: ')     \
                   .append_description_of(matcher)  \
                   .append_text('\n     but: ')
        matcher.describe_mismatch(actual, description)
        description.append_text('\n')
        raise AssertionError(description)


def _assert_bool(assertion, reason=None):
    if not assertion:
        if not reason:
            reason = 'Assertion failed'
        raise AssertionError(reason)
