# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Tests for parts of our release automation system.
"""


import os

from pkg_resources import parse_requirements
from setuptools.dist import Distribution
import twisted
from twisted.trial.unittest import SynchronousTestCase

from twisted.python import _setup, filepath
from twisted.python.compat import _PY3
from twisted.python._setup import (
    BuildPy3,
    getSetupArgs,
    _longDescriptionArgsFromReadme,
    ConditionalExtension,
    _EXTRAS_REQUIRE,
    )



class SetupTests(SynchronousTestCase):
    """
    Tests for L{getSetupArgs}.
    """

    def test_conditionalExtensions(self):
        """
        Will return the arguments with a custom build_ext which knows how to
        check whether they should be built.
        """
        good_ext = ConditionalExtension("whatever", ["whatever.c"],
                                        condition=lambda b: True)
        bad_ext = ConditionalExtension("whatever", ["whatever.c"],
                                       condition=lambda b: False)

        args = getSetupArgs(extensions=[good_ext, bad_ext], readme=None)

        # ext_modules should be set even though it's not used.  See comment
        # in getSetupArgs
        self.assertEqual(args["ext_modules"], [good_ext, bad_ext])
        cmdclass = args["cmdclass"]
        build_ext = cmdclass["build_ext"]
        builder = build_ext(Distribution())
        builder.prepare_extensions()
        self.assertEqual(builder.extensions, [good_ext])


    def test_win32Definition(self):
        """
        When building on Windows NT, the WIN32 macro will be defined as 1 on
        the extensions.
        """
        ext = ConditionalExtension("whatever", ["whatever.c"],
                                   define_macros=[("whatever", 2)])

        args = getSetupArgs(extensions=[ext], readme=None)

        builder = args["cmdclass"]["build_ext"](Distribution())
        self.patch(os, "name", "nt")
        builder.prepare_extensions()
        self.assertEqual(ext.define_macros, [("whatever", 2), ("WIN32", 1)])



class OptionalDependenciesTests(SynchronousTestCase):
    """
    Tests for L{_EXTRAS_REQUIRE}
    """

    def test_distributeTakesExtrasRequire(self):
        """
        Setuptools' Distribution object parses and stores its C{extras_require}
        argument as an attribute.

        Requirements for install_requires/setup_requires can specified as:
         * a single requirement as a string, such as:
           {'im_an_extra_dependency': 'thing'}
         * a series of requirements as a list, such as:
           {'im_an_extra_dependency': ['thing']}
         * a series of requirements as a multi-line string, such as:
           {'im_an_extra_dependency': '''
                                      thing
                                      '''}

        The extras need to be parsed with pkg_resources.parse_requirements(),
        which returns a generator.
        """
        extras = dict(im_an_extra_dependency="thing")
        attrs = dict(extras_require=extras)
        distribution = Distribution(attrs)

        def canonicalizeExtras(myExtras):
            parsedExtras = {}
            for name, val in myExtras.items():
                parsedExtras[name] = list(parse_requirements(val))
            return parsedExtras

        self.assertEqual(
            canonicalizeExtras(extras),
            canonicalizeExtras(distribution.extras_require)
        )


    def test_extrasRequireDictContainsKeys(self):
        """
        L{_EXTRAS_REQUIRE} contains options for all documented extras: C{dev},
        C{tls}, C{conch}, C{soap}, C{serial}, C{all_non_platform},
        C{macos_platform}, and C{windows_platform}.
        """
        self.assertIn('dev', _EXTRAS_REQUIRE)
        self.assertIn('tls', _EXTRAS_REQUIRE)
        self.assertIn('conch', _EXTRAS_REQUIRE)
        self.assertIn('soap', _EXTRAS_REQUIRE)
        self.assertIn('serial', _EXTRAS_REQUIRE)
        self.assertIn('all_non_platform', _EXTRAS_REQUIRE)
        self.assertIn('macos_platform', _EXTRAS_REQUIRE)
        self.assertIn('osx_platform', _EXTRAS_REQUIRE)  # Compat for macOS
        self.assertIn('windows_platform', _EXTRAS_REQUIRE)
        self.assertIn('http2', _EXTRAS_REQUIRE)


    def test_extrasRequiresDevDeps(self):
        """
        L{_EXTRAS_REQUIRE}'s C{dev} extra contains setuptools requirements for
        the tools required for Twisted development.
        """
        deps = _EXTRAS_REQUIRE['dev']
        self.assertIn('pyflakes >= 1.0.0', deps)
        self.assertIn('twisted-dev-tools >= 0.0.2', deps)
        self.assertIn('python-subunit', deps)
        self.assertIn('sphinx >= 1.3.1', deps)
        if not _PY3:
            self.assertIn('twistedchecker >= 0.4.0', deps)
            self.assertIn('pydoctor >= 16.2.0', deps)


    def test_extrasRequiresTlsDeps(self):
        """
        L{_EXTRAS_REQUIRE}'s C{tls} extra contains setuptools requirements for
        the packages required to make Twisted's transport layer security fully
        work for both clients and servers.
        """
        deps = _EXTRAS_REQUIRE['tls']
        self.assertIn('pyopenssl >= 16.0.0', deps)
        self.assertIn('service_identity >= 18.1.0', deps)
        self.assertIn('idna >= 0.6, != 2.3', deps)


    def test_extrasRequiresConchDeps(self):
        """
        L{_EXTRAS_REQUIRE}'s C{conch} extra contains setuptools requirements
        for the packages required to make Twisted Conch's secure shell server
        work.
        """
        deps = _EXTRAS_REQUIRE['conch']
        self.assertIn('pyasn1', deps)
        self.assertIn('cryptography >= 2.5', deps)
        self.assertIn('appdirs >= 1.4.0', deps)


    def test_extrasRequiresSoapDeps(self):
        """
        L{_EXTRAS_REQUIRE}' C{soap} extra contains setuptools requirements for
        the packages required to make the C{twisted.web.soap} module function.
        """
        self.assertIn(
            'soappy',
            _EXTRAS_REQUIRE['soap']
        )


    def test_extrasRequiresSerialDeps(self):
        """
        L{_EXTRAS_REQUIRE}'s C{serial} extra contains setuptools requirements
        for the packages required to make Twisted's serial support work.
        """
        self.assertIn(
            'pyserial >= 3.0',
            _EXTRAS_REQUIRE['serial']
        )


    def test_extrasRequiresHttp2Deps(self):
        """
        L{_EXTRAS_REQUIRE}'s C{http2} extra contains setuptools requirements
        for the packages required to make Twisted HTTP/2 support work.
        """
        deps = _EXTRAS_REQUIRE['http2']
        self.assertIn('h2 >= 3.0, < 4.0', deps)
        self.assertIn('priority >= 1.1.0, < 2.0', deps)


    def test_extrasRequiresAllNonPlatformDeps(self):
        """
        L{_EXTRAS_REQUIRE}'s C{all_non_platform} extra contains setuptools
        requirements for all of Twisted's optional dependencies which work on
        all supported operating systems.
        """
        deps = _EXTRAS_REQUIRE['all_non_platform']
        self.assertIn('pyopenssl >= 16.0.0', deps)
        self.assertIn('service_identity >= 18.1.0', deps)
        self.assertIn('idna >= 0.6, != 2.3', deps)
        self.assertIn('pyasn1', deps)
        self.assertIn('cryptography >= 2.5', deps)
        self.assertIn('soappy', deps)
        self.assertIn('pyserial >= 3.0', deps)
        self.assertIn('appdirs >= 1.4.0', deps)
        self.assertIn('h2 >= 3.0, < 4.0', deps)
        self.assertIn('priority >= 1.1.0, < 2.0', deps)


    def test_extrasRequiresMacosPlatformDeps(self):
        """
        L{_EXTRAS_REQUIRE}'s C{macos_platform} extra contains setuptools
        requirements for all of Twisted's optional dependencies usable on the
        macOS platform.
        """
        deps = _EXTRAS_REQUIRE['macos_platform']
        self.assertIn('pyopenssl >= 16.0.0', deps)
        self.assertIn('service_identity >= 18.1.0', deps)
        self.assertIn('idna >= 0.6, != 2.3', deps)
        self.assertIn('pyasn1', deps)
        self.assertIn('cryptography >= 2.5', deps)
        self.assertIn('soappy', deps)
        self.assertIn('pyserial >= 3.0', deps)
        self.assertIn('h2 >= 3.0, < 4.0', deps)
        self.assertIn('priority >= 1.1.0, < 2.0', deps)
        self.assertIn('pyobjc-core', deps)


    def test_extrasRequireMacOSXPlatformDeps(self):
        """
        L{_EXTRAS_REQUIRE}'s C{osx_platform} is an alias to C{macos_platform}.
        """
        self.assertEqual(_EXTRAS_REQUIRE['macos_platform'],
                         _EXTRAS_REQUIRE['osx_platform'])


    def test_extrasRequiresWindowsPlatformDeps(self):
        """
        L{_EXTRAS_REQUIRE}'s C{windows_platform} extra contains setuptools
        requirements for all of Twisted's optional dependencies usable on the
        Microsoft Windows platform.
        """
        deps = _EXTRAS_REQUIRE['windows_platform']
        self.assertIn('pyopenssl >= 16.0.0', deps)
        self.assertIn('service_identity >= 18.1.0', deps)
        self.assertIn('idna >= 0.6, != 2.3', deps)
        self.assertIn('pyasn1', deps)
        self.assertIn('cryptography >= 2.5', deps)
        self.assertIn('soappy', deps)
        self.assertIn('pyserial >= 3.0', deps)
        self.assertIn('h2 >= 3.0, < 4.0', deps)
        self.assertIn('priority >= 1.1.0, < 2.0', deps)
        self.assertIn('pywin32', deps)



class FakeModule(object):
    """
    A fake module, suitable for dependency injection in testing.
    """
    def __init__(self, attrs):
        """
        Initializes a fake module.

        @param attrs: The attrs that will be accessible on the module.
        @type attrs: C{dict} of C{str} (Python names) to objects
        """
        self._attrs = attrs


    def __getattr__(self, name):
        """
        Gets an attribute of this fake module from its attrs.

        @raise AttributeError: When the requested attribute is missing.
        """
        try:
            return self._attrs[name]
        except KeyError:
            raise AttributeError()



fakeCPythonPlatform = FakeModule({"python_implementation": lambda: "CPython"})
fakeOtherPlatform = FakeModule({"python_implementation": lambda: "lvhpy"})



class WithPlatformTests(SynchronousTestCase):
    """
    Tests for L{_checkCPython} when used with a (fake) C{platform} module.
    """
    def test_cpython(self):
        """
        L{_checkCPython} returns C{True} when C{platform.python_implementation}
        says we're running on CPython.
        """
        self.assertTrue(_setup._checkCPython(platform=fakeCPythonPlatform))


    def test_other(self):
        """
        L{_checkCPython} returns C{False} when C{platform.python_implementation}
        says we're not running on CPython.
        """
        self.assertFalse(_setup._checkCPython(platform=fakeOtherPlatform))



class BuildPy3Tests(SynchronousTestCase):
    """
    Tests for L{BuildPy3}.
    """
    maxDiff = None

    if not _PY3:
        skip = "BuildPy3 setuptools command used with Python 3 only."

    def test_find_package_modules(self):
        """
        Will filter the found modules excluding the modules listed in
        L{twisted.python.dist3}.
        """
        distribution = Distribution()
        distribution.script_name = 'setup.py'
        distribution.script_args = 'build_py'
        builder = BuildPy3(distribution)

        # Rig the dist3 data so that we can reduce the scope of this test and
        # reduce the risk of getting false failures, while doing a minimum
        # level of patching.
        self.patch(
            _setup,
            'notPortedModules',
            [
                "twisted.spread.test.test_pbfailure",
            ],
            )
        twistedPackageDir = filepath.FilePath(twisted.__file__).parent()
        packageDir = twistedPackageDir.child("spread").child("test")

        result = builder.find_package_modules('twisted.spread.test',
                                              packageDir.path)

        self.assertEqual(sorted([
            ('twisted.spread.test', '__init__',
                packageDir.child('__init__.py').path),
            ('twisted.spread.test', 'test_banana',
                packageDir.child('test_banana.py').path),
            ('twisted.spread.test', 'test_jelly',
                packageDir.child('test_jelly.py').path),
            ('twisted.spread.test', 'test_pb',
                packageDir.child('test_pb.py').path),
            ]),
            sorted(result),
        )



class LongDescriptionTests(SynchronousTestCase):
    """
    Tests for C{_getLongDescriptionArgs()}

    Note that the validity of the reStructuredText syntax is tested separately
    using L{twine check} in L{tox.ini}.
    """
    def test_generate(self):
        """
        L{_longDescriptionArgsFromReadme()} outputs a L{long_description} in
        reStructuredText format. Local links are transformed into absolute ones
        that point at the Twisted GitHub repository.
        """
        path = self.mktemp()
        with open(path, 'w') as f:
            f.write('\n'.join([
                'Twisted',
                '=======',
                '',
                'Changes: `NEWS <NEWS.rst>`_.',
                "Read `the docs <https://twistedmatrix.com/documents/>`_.\n",
            ]))

        self.assertEqual({
            'long_description': '''\
Twisted
=======

Changes: `NEWS <https://github.com/twisted/twisted/blob/trunk/NEWS.rst>`_.
Read `the docs <https://twistedmatrix.com/documents/>`_.
''',
            'long_description_content_type': 'text/x-rst',
        }, _longDescriptionArgsFromReadme(path))
