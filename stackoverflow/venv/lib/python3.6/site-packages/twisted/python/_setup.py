# -*- test-case-name: twisted.python.test.test_setup -*-
# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

# pylint: disable=I0011,C0103,C9302,W9401,W9402

"""
Setuptools convenience functionality.

This file must not import anything from Twisted, as it is loaded by C{exec} in
C{setup.py}. If you need compatibility functions for this code, duplicate them
here.

@var _EXTRA_OPTIONS: These are the actual package names and versions that will
    be used by C{extras_require}.  This is not passed to setup directly so that
    combinations of the packages can be created without the need to copy
    package names multiple times.

@var _EXTRAS_REQUIRE: C{extras_require} is a dictionary of items that can be
    passed to setup.py to install optional dependencies.  For example, to
    install the optional dev dependencies one would type::

        pip install -e ".[dev]"

    This has been supported by setuptools since 0.5a4.

@var _PLATFORM_INDEPENDENT: A list of all optional cross-platform dependencies,
    as setuptools version specifiers, used to populate L{_EXTRAS_REQUIRE}.

@var _EXTENSIONS: The list of L{ConditionalExtension} used by the setup
    process.

@var notPortedModules: Modules that are not yet ported to Python 3.
"""

import io
import os
import platform
import re
import sys

from distutils.command import build_ext
from distutils.errors import CompileError
from setuptools import Extension, find_packages
from setuptools.command.build_py import build_py

# Do not replace this with t.p.compat imports, this file must not import
# from Twisted. See the docstring.
if sys.version_info < (3, 0):
    _PY3 = False
else:
    _PY3 = True

STATIC_PACKAGE_METADATA = dict(
    name="Twisted",
    description="An asynchronous networking framework written in Python",
    author="Twisted Matrix Laboratories",
    author_email="twisted-python@twistedmatrix.com",
    maintainer="Glyph Lefkowitz",
    maintainer_email="glyph@twistedmatrix.com",
    url="https://twistedmatrix.com/",
    project_urls={
        'Documentation': 'https://twistedmatrix.com/documents/current/',
        'Source': 'https://github.com/twisted/twisted',
        'Issues': 'https://twistedmatrix.com/trac/report',
    },
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
)


_dev = [
    'pyflakes >= 1.0.0',
    'twisted-dev-tools >= 0.0.2',
    'python-subunit',
    'sphinx >= 1.3.1',
    'towncrier >= 17.4.0'
]

if not _PY3:
    # These modules do not yet work on Python 3.
    _dev += [
        'twistedchecker >= 0.4.0',
        'pydoctor >= 16.2.0',
    ]

_EXTRA_OPTIONS = dict(
    dev=_dev,
    tls=[
        'pyopenssl >= 16.0.0',
        # service_identity 18.1.0 added support for validating IP addresses in
        # certificate subjectAltNames
        'service_identity >= 18.1.0',
        # idna 2.3 introduced some changes that break a few things.  Avoid it.
        # The problems were fixed in 2.4.
        'idna >= 0.6, != 2.3',
    ],
    conch=[
        'pyasn1',
        'cryptography >= 2.5',
        'appdirs >= 1.4.0',
        'bcrypt >= 3.0.0',
    ],
    soap=['soappy'],
    serial=['pyserial >= 3.0'],
    macos=['pyobjc-core',
         'pyobjc-framework-CFNetwork',
         'pyobjc-framework-Cocoa'],
    windows=['pywin32'],
    http2=['h2 >= 3.0, < 4.0',
           'priority >= 1.1.0, < 2.0'],
)

_PLATFORM_INDEPENDENT = (
    _EXTRA_OPTIONS['tls'] +
    _EXTRA_OPTIONS['conch'] +
    _EXTRA_OPTIONS['soap'] +
    _EXTRA_OPTIONS['serial'] +
    _EXTRA_OPTIONS['http2']
)

_EXTRAS_REQUIRE = {
    'dev': _EXTRA_OPTIONS['dev'],
    'tls': _EXTRA_OPTIONS['tls'],
    'conch': _EXTRA_OPTIONS['conch'],
    'soap': _EXTRA_OPTIONS['soap'],
    'serial': _EXTRA_OPTIONS['serial'],
    'http2': _EXTRA_OPTIONS['http2'],
    'all_non_platform': _PLATFORM_INDEPENDENT,
    'macos_platform': (
        _EXTRA_OPTIONS['macos'] + _PLATFORM_INDEPENDENT
    ),
    'windows_platform': (
        _EXTRA_OPTIONS['windows'] + _PLATFORM_INDEPENDENT
    ),
}
_EXTRAS_REQUIRE['osx_platform'] = _EXTRAS_REQUIRE['macos_platform']

# Scripts provided by Twisted on Python 2 and 3.
_CONSOLE_SCRIPTS = [
    "ckeygen = twisted.conch.scripts.ckeygen:run",
    "cftp = twisted.conch.scripts.cftp:run",
    "conch = twisted.conch.scripts.conch:run",
    "mailmail = twisted.mail.scripts.mailmail:run",
    "pyhtmlizer = twisted.scripts.htmlizer:run",
    "tkconch = twisted.conch.scripts.tkconch:run",
    "trial = twisted.scripts.trial:run",
    "twist = twisted.application.twist._twist:Twist.main",
    "twistd = twisted.scripts.twistd:run",
    ]



class ConditionalExtension(Extension, object):
    """
    An extension module that will only be compiled if certain conditions are
    met.

    @param condition: A callable of one argument which returns True or False to
        indicate whether the extension should be built. The argument is an
        instance of L{build_ext_twisted}, which has useful methods for checking
        things about the platform.
    """
    def __init__(self, *args, **kwargs):
        self.condition = kwargs.pop("condition", lambda builder: True)
        Extension.__init__(self, *args, **kwargs)



# The C extensions used for Twisted.
_EXTENSIONS = [
    ConditionalExtension(
        "twisted.test.raiser",
        sources=["src/twisted/test/raiser.c"],
        condition=lambda _: _isCPython),

    ConditionalExtension(
        "twisted.internet.iocpreactor.iocpsupport",
        sources=[
            "src/twisted/internet/iocpreactor/iocpsupport/iocpsupport.c",
            "src/twisted/internet/iocpreactor/iocpsupport/winsock_pointers.c",
            ],
        libraries=["ws2_32"],
        condition=lambda _: _isCPython and sys.platform == "win32"),

    ConditionalExtension(
        "twisted.python._sendmsg",
        sources=["src/twisted/python/_sendmsg.c"],
        condition=lambda _: not _PY3 and sys.platform != "win32"),
    ]



def _checkPythonVersion():
    """
    Fail if we detect a version of Python we don't support.
    """
    version = getattr(sys, "version_info", (0,))
    if version < (2, 7):
        raise ImportError("Twisted requires Python 2.7 or later.")
    elif version >= (3, 0) and version < (3, 5):
        raise ImportError("Twisted on Python 3 requires Python 3.5 or later.")



def _longDescriptionArgsFromReadme(readme):
    """
    Generate a PyPI long description from the readme.

    @param readme: Path to the readme reStructuredText file.
    @type readme: C{str}

    @return: Keyword arguments to be passed to C{setuptools.setup()}.
    @rtype: C{str}
    """
    with io.open(readme, encoding='utf-8') as f:
        readmeRst = f.read()

    # Munge links of the form `NEWS <NEWS.rst>`_ to point at the appropriate
    # location on GitHub so that they function when the long description is
    # displayed on PyPI.
    longDesc = re.sub(
        r'`([^`]+)\s+<(?!https?://)([^>]+)>`_',
        r'`\1 <https://github.com/twisted/twisted/blob/trunk/\2>`_',
        readmeRst,
        flags=re.I,
    )

    return {
        'long_description': longDesc,
        'long_description_content_type': 'text/x-rst',
    }



def getSetupArgs(extensions=_EXTENSIONS, readme='README.rst'):
    """
    Generate arguments for C{setuptools.setup()}

    @param extensions: C extension modules to maybe build. This argument is to
        be used for testing.
    @type extensions: C{list} of C{ConditionalExtension}

    @param readme: Path to the readme reStructuredText file. This argument is
        to be used for testing.
    @type readme: C{str}

    @return: The keyword arguments to be used by the setup method.
    @rtype: L{dict}
    """
    _checkPythonVersion()

    arguments = STATIC_PACKAGE_METADATA.copy()
    if readme:
        arguments.update(_longDescriptionArgsFromReadme(readme))

    # This is a workaround for distutils behavior; ext_modules isn't
    # actually used by our custom builder.  distutils deep-down checks
    # to see if there are any ext_modules defined before invoking
    # the build_ext command.  We need to trigger build_ext regardless
    # because it is the thing that does the conditional checks to see
    # if it should build any extensions.  The reason we have to delay
    # the conditional checks until then is that the compiler objects
    # are not yet set up when this code is executed.
    arguments["ext_modules"] = extensions
    # Use custome class to build the extensions.
    class my_build_ext(build_ext_twisted):
        conditionalExtensions = extensions
    command_classes = {
        'build_ext': my_build_ext,
    }

    if sys.version_info[0] >= 3:
        command_classes['build_py'] = BuildPy3

    requirements = [
        "zope.interface >= 4.4.2",
        "constantly >= 15.1",
        "incremental >= 16.10.1",
        "Automat >= 0.3.0",
        "hyperlink >= 17.1.1",
        "PyHamcrest >= 1.9.0",
        "attrs >= 17.4.0",
    ]

    arguments.update(dict(
        packages=find_packages("src"),
        use_incremental=True,
        setup_requires=["incremental >= 16.10.1"],
        install_requires=requirements,
        entry_points={
            'console_scripts': _CONSOLE_SCRIPTS
        },
        cmdclass=command_classes,
        include_package_data=True,
        exclude_package_data={
            "": ["*.c", "*.h", "*.pxi", "*.pyx", "build.bat"],
        },
        zip_safe=False,
        extras_require=_EXTRAS_REQUIRE,
        package_dir={"": "src"},
    ))

    return arguments



class BuildPy3(build_py, object):
    """
    A version of build_py that doesn't install the modules that aren't yet
    ported to Python 3.
    """
    def find_package_modules(self, package, package_dir):
        modules = [
            module for module
            in build_py.find_package_modules(self, package, package_dir)
            if ".".join([module[0], module[1]]) not in notPortedModules]
        return modules



## Helpers and distutil tweaks


class build_ext_twisted(build_ext.build_ext, object):
    """
    Allow subclasses to easily detect and customize Extensions to
    build at install-time.
    """

    def prepare_extensions(self):
        """
        Prepare the C{self.extensions} attribute (used by
        L{build_ext.build_ext}) by checking which extensions in
        I{conditionalExtensions} should be built.  In addition, if we are
        building on NT, define the WIN32 macro to 1.
        """
        # always define WIN32 under Windows
        if os.name == 'nt':
            self.define_macros = [("WIN32", 1)]
        else:
            self.define_macros = []

        # On Solaris 10, we need to define the _XOPEN_SOURCE and
        # _XOPEN_SOURCE_EXTENDED macros to build in order to gain access to
        # the msg_control, msg_controllen, and msg_flags members in
        # sendmsg.c. (according to
        # https://stackoverflow.com/questions/1034587).  See the documentation
        # of X/Open CAE in the standards(5) man page of Solaris.
        if sys.platform.startswith('sunos'):
            self.define_macros.append(('_XOPEN_SOURCE', 1))
            self.define_macros.append(('_XOPEN_SOURCE_EXTENDED', 1))

        self.extensions = [
            x for x in self.conditionalExtensions if x.condition(self)
        ]

        for ext in self.extensions:
            ext.define_macros.extend(self.define_macros)


    def build_extensions(self):
        """
        Check to see which extension modules to build and then build them.
        """
        self.prepare_extensions()
        build_ext.build_ext.build_extensions(self)


    def _remove_conftest(self):
        for filename in ("conftest.c", "conftest.o", "conftest.obj"):
            try:
                os.unlink(filename)
            except EnvironmentError:
                pass


    def _compile_helper(self, content):
        conftest = open("conftest.c", "w")
        try:
            with conftest:
                conftest.write(content)

            try:
                self.compiler.compile(["conftest.c"], output_dir='')
            except CompileError:
                return False
            return True
        finally:
            self._remove_conftest()


    def _check_header(self, header_name):
        """
        Check if the given header can be included by trying to compile a file
        that contains only an #include line.
        """
        self.compiler.announce("checking for {} ...".format(header_name), 0)
        return self._compile_helper("#include <{}>\n".format(header_name))



def _checkCPython(sys=sys, platform=platform):
    """
    Checks if this implementation is CPython.

    This uses C{platform.python_implementation}.

    This takes C{sys} and C{platform} kwargs that by default use the real
    modules. You shouldn't care about these -- they are for testing purposes
    only.

    @return: C{False} if the implementation is definitely not CPython, C{True}
        otherwise.
    """
    return platform.python_implementation() == "CPython"


_isCPython = _checkCPython()

notPortedModules = [
    "twisted.mail.alias",
    "twisted.mail.bounce",
    "twisted.mail.mail",
    "twisted.mail.maildir",
    "twisted.mail.pb",
    "twisted.mail.relaymanager",
    "twisted.mail.scripts.__init__",
    "twisted.mail.tap",
    "twisted.mail.test.test_bounce",
    "twisted.mail.test.test_mail",
    "twisted.mail.test.test_options",
    "twisted.mail.test.test_scripts",
    "twisted.news.__init__",
    "twisted.news.database",
    "twisted.news.news",
    "twisted.news.nntp",
    "twisted.news.tap",
    "twisted.news.test.__init__",
    "twisted.news.test.test_database",
    "twisted.news.test.test_news",
    "twisted.news.test.test_nntp",
    "twisted.plugins.twisted_mail",
    "twisted.plugins.twisted_news",
    "twisted.protocols.shoutcast",
    "twisted.python._pydoctor",
    "twisted.python.finalize",
    "twisted.python.hook",
    "twisted.python.test.cmodulepullpipe",
    "twisted.python.test.test_pydoctor",
    "twisted.python.test.test_win32",
    "twisted.test.test_hook",
    "twisted.web.soap",
    "twisted.web.test.test_soap",
]
