# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Commands for reporting test success of failure to the manager.

@since: 12.3
"""

from twisted.protocols.amp import Command, String, Boolean, ListOf, Unicode
from twisted.python.compat import _PY3

NativeString = Unicode if _PY3 else String



class AddSuccess(Command):
    """
    Add a success.
    """
    arguments = [(b'testName', NativeString())]
    response = [(b'success', Boolean())]



class AddError(Command):
    """
    Add an error.
    """
    arguments = [(b'testName', NativeString()),
                 (b'error', NativeString()),
                 (b'errorClass', NativeString()),
                 (b'frames', ListOf(NativeString()))]
    response = [(b'success', Boolean())]



class AddFailure(Command):
    """
    Add a failure.
    """
    arguments = [(b'testName', NativeString()),
                 (b'fail', NativeString()),
                 (b'failClass', NativeString()),
                 (b'frames', ListOf(NativeString()))]
    response = [(b'success', Boolean())]



class AddSkip(Command):
    """
    Add a skip.
    """
    arguments = [(b'testName', NativeString()),
                 (b'reason', NativeString())]
    response = [(b'success', Boolean())]



class AddExpectedFailure(Command):
    """
    Add an expected failure.
    """
    arguments = [(b'testName', NativeString()),
                 (b'error', NativeString()),
                 (b'todo', NativeString())]
    response = [(b'success', Boolean())]



class AddUnexpectedSuccess(Command):
    """
    Add an unexpected success.
    """
    arguments = [(b'testName', NativeString()),
                 (b'todo', NativeString())]
    response = [(b'success', Boolean())]



class TestWrite(Command):
    """
    Write test log.
    """
    arguments = [(b'out', NativeString())]
    response = [(b'success', Boolean())]
