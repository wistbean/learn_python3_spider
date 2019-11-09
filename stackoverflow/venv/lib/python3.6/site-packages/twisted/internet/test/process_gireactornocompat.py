import sys

# Override theSystemPath so it throws KeyError on gi.pygtkcompat:
from twisted.python import modules
modules.theSystemPath = modules.PythonPath([], moduleDict={})

# Now, when we import gireactor it shouldn't use pygtkcompat, and should
# instead prevent gobject from being importable:
from twisted.internet import gireactor
for name in gireactor._PYGTK_MODULES:
    if sys.modules[name] is not None:
        sys.stdout.write("failure, sys.modules[%r] is %r, instead of None" %
                         (name, sys.modules["gobject"]))
        sys.exit(0)

try:
    import gobject
except ImportError:
    sys.stdout.write("success")
else:
    sys.stdout.write("failure: %s was imported" % (gobject.__path__,))
