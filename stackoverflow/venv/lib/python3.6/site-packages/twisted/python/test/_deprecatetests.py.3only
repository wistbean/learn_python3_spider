# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

import inspect

from twisted.python.deprecate import _passedSignature
from twisted.trial.unittest import SynchronousTestCase


class KeywordOnlyTests(SynchronousTestCase):
    """
    Keyword only arguments (PEP 3102).
    """
    def checkPassed(self, func, *args, **kw):
        """
        Test an invocation of L{passed} with the given function, arguments, and
        keyword arguments.

        @param func: A function whose argspec to pass to L{_passed}.
        @type func: A callable.

        @param args: The arguments which could be passed to L{func}.

        @param kw: The keyword arguments which could be passed to L{func}.

        @return: L{_passed}'s return value
        @rtype: L{dict}
        """
        return _passedSignature(inspect.signature(func), args, kw)


    def test_passedKeywordOnly(self):
        """
        Keyword only arguments follow varargs.
        They are specified in PEP 3102.
        """
        def func1(*a, b=True):
            """
            b is a keyword-only argument, with a default value.
            """

        def func2(*a, b=True, c, d, e):
            """
            b, c, d, e  are keyword-only arguments.
            b has a default value.
            """

        self.assertEqual(self.checkPassed(func1, 1, 2, 3),
                         dict(a=(1, 2, 3), b=True))
        self.assertEqual(self.checkPassed(func1, 1, 2, 3, b=False),
                         dict(a=(1, 2, 3), b=False))
        self.assertEqual(self.checkPassed(func2,
                         1, 2, b=False, c=1, d=2, e=3),
                         dict(a=(1, 2), b=False, c=1, d=2, e=3))
        self.assertRaises(TypeError, self.checkPassed,
                          func2, 1, 2, b=False, c=1, d=2)
