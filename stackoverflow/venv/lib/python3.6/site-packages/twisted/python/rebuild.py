# -*- test-case-name: twisted.test.test_rebuild -*-
# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.


"""
*Real* reloading support for Python.
"""

# System Imports
import sys
import types
import time
import linecache

from imp import reload

try:
    # Python 2
    from types import InstanceType
except ImportError:
    # Python 3
    pass

# Sibling Imports
from twisted.python import log, reflect
from twisted.python.compat import _PY3

lastRebuild = time.time()

def _isClassType(t):
    """
    Compare to types.ClassType in a py2/3-compatible way

    Python 2 used comparison to types.ClassType to check for old-style
    classes Python 3 has no concept of old-style classes, so if
    ClassType doesn't exist, it can't be an old-style class - return
    False in that case.

    Note that the type() of new-style classes is NOT ClassType, and
    so this should return False for new-style classes in python 2
    as well.
    """
    _ClassType = getattr(types, 'ClassType', None)
    if _ClassType is None:
        return False
    return t == _ClassType



class Sensitive(object):
    """
    A utility mixin that's sensitive to rebuilds.

    This is a mixin for classes (usually those which represent collections of
    callbacks) to make sure that their code is up-to-date before running.
    """

    lastRebuild = lastRebuild

    def needRebuildUpdate(self):
        yn = (self.lastRebuild < lastRebuild)
        return yn


    def rebuildUpToDate(self):
        self.lastRebuild = time.time()


    def latestVersionOf(self, anObject):
        """
        Get the latest version of an object.

        This can handle just about anything callable; instances, functions,
        methods, and classes.
        """
        t = type(anObject)
        if t == types.FunctionType:
            return latestFunction(anObject)
        elif t == types.MethodType:
            if anObject.__self__ is None:
                return getattr(anObject.im_class, anObject.__name__)
            else:
                return getattr(anObject.__self__, anObject.__name__)
        elif not _PY3 and t == InstanceType:
            # Kick it, if it's out of date.
            getattr(anObject, 'nothing', None)
            return anObject
        elif _isClassType(t):
            return latestClass(anObject)
        else:
            log.msg('warning returning anObject!')
            return anObject

_modDictIDMap = {}

def latestFunction(oldFunc):
    """
    Get the latest version of a function.
    """
    # This may be CPython specific, since I believe jython instantiates a new
    # module upon reload.
    dictID = id(oldFunc.__globals__)
    module = _modDictIDMap.get(dictID)
    if module is None:
        return oldFunc
    return getattr(module, oldFunc.__name__)



def latestClass(oldClass):
    """
    Get the latest version of a class.
    """
    module = reflect.namedModule(oldClass.__module__)
    newClass = getattr(module, oldClass.__name__)
    newBases = [latestClass(base) for base in newClass.__bases__]

    try:
        # This makes old-style stuff work
        newClass.__bases__ = tuple(newBases)
        return newClass
    except TypeError:
        if newClass.__module__ in ("__builtin__", "builtins"):
            # __builtin__ members can't be reloaded sanely
            return newClass

        ctor = type(newClass)
        # The value of type(newClass) is the metaclass
        # in both Python 2 and 3, except if it was old-style.
        if _isClassType(ctor):
            ctor = getattr(newClass, '__metaclass__', type)
        return ctor(newClass.__name__, tuple(newBases),
                    dict(newClass.__dict__))



class RebuildError(Exception):
    """
    Exception raised when trying to rebuild a class whereas it's not possible.
    """



def updateInstance(self):
    """
    Updates an instance to be current.
    """
    self.__class__ = latestClass(self.__class__)



def __injectedgetattr__(self, name):
    """
    A getattr method to cause a class to be refreshed.
    """
    if name == '__del__':
        raise AttributeError("Without this, Python segfaults.")
    updateInstance(self)
    log.msg("(rebuilding stale {} instance ({}))".format(
            reflect.qual(self.__class__), name))
    result = getattr(self, name)
    return result



def rebuild(module, doLog=1):
    """
    Reload a module and do as much as possible to replace its references.
    """
    global lastRebuild
    lastRebuild = time.time()
    if hasattr(module, 'ALLOW_TWISTED_REBUILD'):
        # Is this module allowed to be rebuilt?
        if not module.ALLOW_TWISTED_REBUILD:
            raise RuntimeError("I am not allowed to be rebuilt.")
    if doLog:
        log.msg('Rebuilding {}...'.format(str(module.__name__)))

    # Safely handle adapter re-registration
    from twisted.python import components
    components.ALLOW_DUPLICATES = True

    d = module.__dict__
    _modDictIDMap[id(d)] = module
    newclasses = {}
    classes = {}
    functions = {}
    values = {}
    if doLog:
        log.msg('  (scanning {}): '.format(str(module.__name__)))
    for k, v in d.items():
        if _isClassType(type(v)):
            # ClassType exists on Python 2.x and earlier.
            # Failure condition -- instances of classes with buggy
            # __hash__/__cmp__ methods referenced at the module level...
            if v.__module__ == module.__name__:
                classes[v] = 1
                if doLog:
                    log.logfile.write("c")
                    log.logfile.flush()
        elif type(v) == types.FunctionType:
            if v.__globals__ is module.__dict__:
                functions[v] = 1
                if doLog:
                    log.logfile.write("f")
                    log.logfile.flush()
        elif isinstance(v, type):
            if v.__module__ == module.__name__:
                newclasses[v] = 1
                if doLog:
                    log.logfile.write("o")
                    log.logfile.flush()

    values.update(classes)
    values.update(functions)
    fromOldModule = values.__contains__
    newclasses = newclasses.keys()
    classes = classes.keys()
    functions = functions.keys()

    if doLog:
        log.msg('')
        log.msg('  (reload   {})'.format(str(module.__name__)))

    # Boom.
    reload(module)
    # Make sure that my traceback printing will at least be recent...
    linecache.clearcache()

    if doLog:
        log.msg('  (cleaning {}): '.format(str(module.__name__)))

    for clazz in classes:
        if getattr(module, clazz.__name__) is clazz:
            log.msg("WARNING: class {} not replaced by reload!".format(
                    reflect.qual(clazz)))
        else:
            if doLog:
                log.logfile.write("x")
                log.logfile.flush()
            clazz.__bases__ = ()
            clazz.__dict__.clear()
            clazz.__getattr__ = __injectedgetattr__
            clazz.__module__ = module.__name__
    if newclasses:
        import gc
    for nclass in newclasses:
        ga = getattr(module, nclass.__name__)
        if ga is nclass:
            log.msg("WARNING: new-class {} not replaced by reload!".format(
                    reflect.qual(nclass)))
        else:
            for r in gc.get_referrers(nclass):
                if getattr(r, '__class__', None) is nclass:
                    r.__class__ = ga
    if doLog:
        log.msg('')
        log.msg('  (fixing   {}): '.format(str(module.__name__)))
    modcount = 0
    for mk, mod in sys.modules.items():
        modcount = modcount + 1
        if mod == module or mod is None:
            continue

        if not hasattr(mod, '__file__'):
            # It's a builtin module; nothing to replace here.
            continue

        if hasattr(mod, '__bundle__'):
            # PyObjC has a few buggy objects which segfault if you hash() them.
            # It doesn't make sense to try rebuilding extension modules like
            # this anyway, so don't try.
            continue

        changed = 0

        for k, v in mod.__dict__.items():
            try:
                hash(v)
            except Exception:
                continue
            if fromOldModule(v):
                if _isClassType(type(v)):
                    if doLog:
                        log.logfile.write("c")
                        log.logfile.flush()
                    nv = latestClass(v)
                else:
                    if doLog:
                        log.logfile.write("f")
                        log.logfile.flush()
                    nv = latestFunction(v)
                changed = 1
                setattr(mod, k, nv)
            else:
                # Replace bases of non-module classes just to be sure.
                if _isClassType(type(v)):
                    for base in v.__bases__:
                        if fromOldModule(base):
                            latestClass(v)
        if doLog and not changed and ((modcount % 10) == 0) :
            log.logfile.write(".")
            log.logfile.flush()

    components.ALLOW_DUPLICATES = False
    if doLog:
        log.msg('')
        log.msg('   Rebuilt {}.'.format(str(module.__name__)))
    return module
