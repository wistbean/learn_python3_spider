# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Twisted Python: Utilities and Enhancements for Python.
"""

from __future__ import absolute_import, division

# Deprecating twisted.python.constants.
from .compat import unicode
from .versions import Version
from .deprecate import deprecatedModuleAttribute

deprecatedModuleAttribute(
    Version("Twisted", 16, 5, 0),
    "Please use constantly from PyPI instead.",
    "twisted.python", "constants")


deprecatedModuleAttribute(
    Version('Twisted', 17, 5, 0),
    "Please use hyperlink from PyPI instead.",
    "twisted.python", "url")


del Version
del deprecatedModuleAttribute
del unicode
