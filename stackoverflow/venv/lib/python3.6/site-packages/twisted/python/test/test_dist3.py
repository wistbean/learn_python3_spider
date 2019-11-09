# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Tests for L{twisted.python.dist3}.
"""

from __future__ import absolute_import, division

import os
import twisted

from twisted.trial.unittest import TestCase
from twisted.python.compat import _PY3
from twisted.python._setup import notPortedModules


class ModulesToInstallTests(TestCase):
    """
    Tests for L{notPortedModules}.
    """
    def test_exist(self):
        """
        All modules listed in L{notPortedModules} exist on Py2.
        """
        root = os.path.dirname(os.path.dirname(twisted.__file__))
        for module in notPortedModules:
            segments = module.split(".")
            segments[-1] += ".py"
            path = os.path.join(root, *segments)
            alternateSegments = module.split(".") + ["__init__.py"]
            packagePath = os.path.join(root, *alternateSegments)
            self.assertTrue(os.path.exists(path) or
                            os.path.exists(packagePath),
                            "Module {0} does not exist".format(module))

    def test_notexist(self):
        """
        All modules listed in L{notPortedModules} do not exist on Py3.
        """
        root = os.path.dirname(os.path.dirname(twisted.__file__))
        for module in notPortedModules:
            segments = module.split(".")
            segments[-1] += ".py"
            path = os.path.join(root, *segments)
            alternateSegments = module.split(".") + ["__init__.py"]
            packagePath = os.path.join(root, *alternateSegments)
            self.assertFalse(os.path.exists(path) or
                             os.path.exists(packagePath),
                             "Module {0} exists".format(module))

    if _PY3:
        test_exist.skip = "Only on Python 2"
    else:
        test_notexist.skip = "Only on Python 3"
