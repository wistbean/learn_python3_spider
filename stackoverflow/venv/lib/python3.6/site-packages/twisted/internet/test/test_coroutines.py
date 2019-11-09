# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
A wrapper for L{twisted.internet.test._awaittests}, as that test module
includes keywords not valid in Pythons before 3.5.
"""

from __future__ import absolute_import, division

from twisted.python.compat import _PY35PLUS, _PY3, execfile
from twisted.python.filepath import FilePath


if _PY35PLUS:
    _path = FilePath(__file__).parent().child("_awaittests.py.3only")

    _g = {"__name__": __name__ + ".3-only.awaittests"}
    execfile(_path.path, _g)
    AwaitTests = _g["AwaitTests"]
else:
    from twisted.trial.unittest import TestCase

    class AwaitTests(TestCase):
        """
        A dummy class to show that this test file was discovered but the tests
        are unable to be ran in this version of Python.
        """
        skip = "async/await is not available before Python 3.5"

        def test_notAvailable(self):
            """
            A skipped test to show that this was not ran because the Python is
            too old.
            """


if _PY3:
    _path = FilePath(__file__).parent().child("_yieldfromtests.py.3only")

    _g = {"__name__": __name__ + ".3-only.yieldfromtests"}
    execfile(_path.path, _g)
    YieldFromTests = _g["YieldFromTests"]
else:
    from twisted.trial.unittest import TestCase

    class YieldFromTests(TestCase):
        """
        A dummy class to show that this test file was discovered but the tests
        are unable to be ran in this version of Python.
        """
        skip = "yield from is not available before Python 3"

        def test_notAvailable(self):
            """
            A skipped test to show that this was not ran because the Python is
            too old.
            """


__all__ = ["AwaitTests", "YieldFromTests"]
