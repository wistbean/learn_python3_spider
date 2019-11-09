# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Tests for miscellaneous behaviors of the top-level L{twisted} package (ie, for
the code in C{twisted/__init__.py}.
"""

from __future__ import division, absolute_import

import sys

from types import ModuleType

from twisted.python._setup import _checkPythonVersion
from twisted.trial.unittest import TestCase


# This is somewhat generally useful and should probably be part of a public API
# somewhere.  See #5977.
class SetAsideModule(object):
    """
    L{SetAsideModule} is a context manager for temporarily removing a module
    from C{sys.modules}.

    @ivar name: The name of the module to remove.
    """
    def __init__(self, name):
        self.name = name


    def _unimport(self, name):
        """
        Find the given module and all of its hierarchically inferior modules in
        C{sys.modules}, remove them from it, and return whatever was found.
        """
        modules = dict([
                (moduleName, module)
                for (moduleName, module)
                in list(sys.modules.items())
                if (moduleName == self.name or
                    moduleName.startswith(self.name + "."))])
        for name in modules:
            del sys.modules[name]
        return modules


    def __enter__(self):
        self.modules = self._unimport(self.name)


    def __exit__(self, excType, excValue, traceback):
        self._unimport(self.name)
        sys.modules.update(self.modules)



def _install(modules):
    """
    Take a mapping defining a package and turn it into real C{ModuleType}
    instances in C{sys.modules}.

    Consider these example::

        a = {"foo": "bar"}
        b = {"twisted": {"__version__": "42.6"}}
        c = {"twisted": {"plugin": {"getPlugins": stub}}}

    C{_install(a)} will place an item into C{sys.modules} with C{"foo"} as the
    key and C{"bar" as the value.

    C{_install(b)} will place an item into C{sys.modules} with C{"twisted"} as
    the key.  The value will be a new module object.  The module will have a
    C{"__version__"} attribute with C{"42.6"} as the value.

    C{_install(c)} will place an item into C{sys.modules} with C{"twisted"} as
    the key.  The value will be a new module object with a C{"plugin"}
    attribute.  An item will also be placed into C{sys.modules} with the key
    C{"twisted.plugin"} which refers to that module object.  That module will
    have an attribute C{"getPlugins"} with a value of C{stub}.

    @param modules: A mapping from names to definitions of modules.  The names
        are native strings like C{"twisted"} or C{"unittest"}.  Values may be
        arbitrary objects.  Any value which is not a dictionary will be added to
        C{sys.modules} unmodified.  Any dictionary value indicates the value is
        a new module and its items define the attributes of that module.  The
        definition of this structure is recursive, so a value in the dictionary
        may be a dictionary to trigger another level of processing.

    @return: L{None}
    """
    result = {}
    _makePackages(None, modules, result)
    sys.modules.update(result)



def _makePackages(parent, attributes, result):
    """
    Construct module objects (for either modules or packages).

    @param parent: L{None} or a module object which is the Python package
        containing all of the modules being created by this function call.  Its
        name will be prepended to the name of all created modules.

    @param attributes: A mapping giving the attributes of the particular module
        object this call is creating.

    @param result: A mapping which is populated with all created module names.
        This is suitable for use in updating C{sys.modules}.

    @return: A mapping of all of the attributes created by this call.  This is
        suitable for populating the dictionary of C{parent}.

    @see: L{_install}.
    """
    attrs = {}
    for (name, value) in list(attributes.items()):
        if parent is None:
            if isinstance(value, dict):
                module = ModuleType(name)
                module.__dict__.update(_makePackages(module, value, result))
                result[name] = module
            else:
                result[name] = value
        else:
            if isinstance(value, dict):
                module = ModuleType(parent.__name__ + '.' + name)
                module.__dict__.update(_makePackages(module, value, result))
                result[parent.__name__ + '.' + name] = module
                attrs[name] = module
            else:
                attrs[name] = value
    return attrs



class RequirementsTests(TestCase):
    """
    Tests for the import-time requirements checking.

    @ivar unsupportedPythonVersion: The newest version of Python 2.x which is
        not supported by Twisted.
    @type unsupportedPythonVersion: C{tuple}

    @ivar supportedPythonVersion: The oldest version of Python 2.x which is
        supported by Twisted.
    @type supportedPythonVersion: C{tuple}

    @ivar Py3unsupportedPythonVersion: The newest version of Python 3.x which
        is not supported by Twisted.
    @type Py3unsupportedPythonVersion: C{tuple}

    @ivar Py3supportedPythonVersion: The oldest version of Python 3.x which is
        supported by Twisted.
    @type supportedPythonVersion: C{tuple}
    """
    unsupportedPythonVersion = (2, 6)
    supportedPythonVersion = (2, 7)
    Py3unsupportedPythonVersion = (3, 4)
    Py3supportedPythonVersion = (3, 5)


    def setUp(self):
        """
        Save the original value of C{sys.version_info} so it can be restored
        after the tests mess with it.
        """
        self.version = sys.version_info


    def tearDown(self):
        """
        Restore the original values saved in L{setUp}.
        """
        sys.version_info = self.version


    def test_oldPython(self):
        """
        L{_checkPythonVersion} raises L{ImportError} when run on a version of
        Python that is too old.
        """
        sys.version_info = self.unsupportedPythonVersion
        with self.assertRaises(ImportError) as raised:
            _checkPythonVersion()
        self.assertEqual("Twisted requires Python %d.%d or later."
                         % self.supportedPythonVersion,
                         str(raised.exception))


    def test_newPython(self):
        """
        L{_checkPythonVersion} returns L{None} when run on a version of Python
        that is sufficiently new.
        """
        sys.version_info = self.supportedPythonVersion
        self.assertIsNone(_checkPythonVersion())


    def test_oldPythonPy3(self):
        """
        L{_checkPythonVersion} raises L{ImportError} when run on a version of
        Python that is too old.
        """
        sys.version_info = self.Py3unsupportedPythonVersion
        with self.assertRaises(ImportError) as raised:
            _checkPythonVersion()
        self.assertEqual("Twisted on Python 3 requires Python %d.%d or later."
                         % self.Py3supportedPythonVersion,
                         str(raised.exception))


    def test_newPythonPy3(self):
        """
        L{_checkPythonVersion} returns L{None} when run on a version of Python
        that is sufficiently new.
        """
        sys.version_info = self.Py3supportedPythonVersion
        self.assertIsNone(_checkPythonVersion())



class MakePackagesTests(TestCase):
    """
    Tests for L{_makePackages}, a helper for populating C{sys.modules} with
    fictional modules.
    """
    def test_nonModule(self):
        """
        A non-C{dict} value in the attributes dictionary passed to L{_makePackages}
        is preserved unchanged in the return value.
        """
        modules = {}
        _makePackages(None, dict(reactor='reactor'), modules)
        self.assertEqual(modules, dict(reactor='reactor'))


    def test_moduleWithAttribute(self):
        """
        A C{dict} value in the attributes dictionary passed to L{_makePackages}
        is turned into a L{ModuleType} instance with attributes populated from
        the items of that C{dict} value.
        """
        modules = {}
        _makePackages(None, dict(twisted=dict(version='123')), modules)
        self.assertIsInstance(modules, dict)
        self.assertIsInstance(modules['twisted'], ModuleType)
        self.assertEqual('twisted', modules['twisted'].__name__)
        self.assertEqual('123', modules['twisted'].version)


    def test_packageWithModule(self):
        """
        Processing of the attributes dictionary is recursive, so a C{dict} value
        it contains may itself contain a C{dict} value to the same effect.
        """
        modules = {}
        _makePackages(None, dict(twisted=dict(web=dict(version='321'))), modules)
        self.assertIsInstance(modules, dict)
        self.assertIsInstance(modules['twisted'], ModuleType)
        self.assertEqual('twisted', modules['twisted'].__name__)
        self.assertIsInstance(modules['twisted'].web, ModuleType)
        self.assertEqual('twisted.web', modules['twisted'].web.__name__)
        self.assertEqual('321', modules['twisted'].web.version)
