from weakref import ref
import re
import sys
from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.compat import is_callable

__author__ = "Per Fagrell"
__copyright__ = "Copyright 2013 hamcrest.org"
__license__ = "BSD, see License.txt"


class Raises(BaseMatcher):
    def __init__(self, expected, pattern=None):
        self.pattern = pattern
        self.expected = expected
        self.actual = None
        self.function = None

    def _matches(self, function):
        if not is_callable(function):
            return False

        self.function = ref(function)
        return self._call_function(function)

    def _call_function(self, function):
        self.actual = None
        try:
            function()
        except Exception:
            self.actual = sys.exc_info()[1]

            if isinstance(self.actual, self.expected):
                if self.pattern is not None:
                    return re.search(self.pattern, str(self.actual)) is not None
                return True
        return False

    def describe_to(self, description):
        description.append_text('Expected a callable raising %s' % self.expected)

    def describe_mismatch(self, item, description):
        if not is_callable(item):
            description.append_text('%s is not callable' % item)
            return

        function = None if self.function is None else self.function()
        if function is None or function is not item:
            self.function = ref(item)
            if not self._call_function(item):
                return

        if self.actual is None:
            description.append_text('No exception raised.')
        elif isinstance(self.actual, self.expected) and self.pattern is not None:
            description.append_text('Correct assertion type raised, but the expected pattern ("%s") not found.' % self.pattern)
            description.append_text('\n          message was: "%s"' % str(self.actual))
        else:
            description.append_text('%s was raised instead' % type(self.actual))


def raises(exception, pattern=None):
    """Matches if the called function raised the expected exception.

    :param exception:  The class of the expected exception
    :param pattern:    Optional regular expression to match exception message.

    Expects the actual to be wrapped by using :py:func:`~hamcrest.core.core.raises.calling`,
    or a callable taking no arguments.
    Optional argument pattern should be a string containing a regular expression.  If provided,
    the string representation of the actual exception - e.g. `str(actual)` - must match pattern.

    Examples::

        assert_that(calling(int).with_args('q'), raises(TypeError))
        assert_that(calling(parse, broken_input), raises(ValueError))
    """
    return Raises(exception, pattern)


class DeferredCallable(object):
    def __init__(self, func):
        self.func = func
        self.args = tuple()
        self.kwargs = {}

    def __call__(self):
        return self.func(*self.args, **self.kwargs)

    def with_args(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        return self


def calling(func):
    """Wrapper for function call that delays the actual execution so that
    :py:func:`~hamcrest.core.core.raises.raises` matcher can catch any thrown exception.

    :param func: The function or method to be called

    The arguments can be provided with a call to the `with_args` function on the returned
    object::

           calling(my_method).with_args(arguments, and_='keywords')
    """
    return DeferredCallable(func)
