# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Twisted Protocols: A collection of internet protocol implementations.
"""

from incremental import Version
from twisted.python.deprecate import deprecatedModuleAttribute


deprecatedModuleAttribute(
    Version('Twisted', 17, 9, 0),
    "There is no replacement for this module.",
    "twisted.protocols", "dict")
