# -*- test-case-name: twisted.trial._dist.test.test_options -*-
#
# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Options handling specific to trial's workers.

@since: 12.3
"""

from twisted.python.filepath import FilePath
from twisted.python.usage import Options
from twisted.scripts.trial import _BasicOptions
from twisted.application.app import ReactorSelectionMixin



class WorkerOptions(_BasicOptions, Options, ReactorSelectionMixin):
    """
    Options forwarded to the trial distributed worker.
    """


    def coverdir(self):
        """
        Return a L{FilePath} representing the directory into which coverage
        results should be written.
        """
        return FilePath('coverage')
