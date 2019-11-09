import unittest, tempfile, shutil

class QueuelibTestCase(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="queuelib-tests-")
        self.qpath = self.mktemp()
        self.qdir = self.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def mktemp(self):
        return tempfile.mktemp(dir=self.tmpdir)

    def mkdtemp(self):
        return tempfile.mkdtemp(dir=self.tmpdir)


def track_closed(cls):
    """Wraps a queue class to track down if close() method was called"""

    class TrackingClosed(cls):

        def __init__(self, *a, **kw):
            super(TrackingClosed, self).__init__(*a, **kw)
            self.closed = False

        def close(self):
            super(TrackingClosed, self).close()
            self.closed = True

    return TrackingClosed
