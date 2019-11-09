from collections import deque

class RoundRobinQueue(object):
    """A round robin queue implemented using multiple internal queues (typically,
    FIFO queues). The internal queue must implement the following methods:
        * push(obj)
        * pop()
        * close()
        * __len__()
    The constructor receives a qfactory argument, which is a callable used to
    instantiate a new (internal) queue when a new key is allocated. The
    qfactory function is called with the key number as first and only
    argument.
    start_keys is a sequence of domains to start with. If the queue was
    previously closed leaving some domain buckets non-empty, those domains
    should be passed in start_keys.

    The queue maintains a fifo queue of keys.  The key that went last is
    poped first and the next queue for that key is then poped.  This allows
    for a round robin
    """

    def __init__(self, qfactory, start_domains=()):
        self.queues = {}
        self.qfactory = qfactory
        for key in start_domains:
            self.queues[key] = self.qfactory(key)

        self.key_queue = deque(start_domains)

    def push(self, obj, key):
        if key not in self.key_queue:
            self.queues[key] = self.qfactory(key)
            self.key_queue.appendleft(key)  # it's new, might as well pop first

        q = self.queues[key]
        q.push(obj) # this may fail (eg. serialization error)

    def pop(self):
        m = None
        # pop until we find a valid object, closing necessary queues
        while m is None:
            try:
                key = self.key_queue.pop()
            except IndexError:
                return

            q = self.queues[key]
            m = q.pop()

            if len(q) == 0:
                del self.queues[key]
                q.close()
            else:
                self.key_queue.appendleft(key)

            if m:
                return m

    def close(self):
        active = []
        for k, q in self.queues.items():
            if len(q):
                active.append(k)
            q.close()
        return active

    def __len__(self):
        return sum(len(x) for x in self.queues.values()) if self.queues else 0
