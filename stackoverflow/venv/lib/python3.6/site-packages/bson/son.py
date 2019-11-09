# Copyright 2009-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tools for creating and manipulating SON, the Serialized Ocument Notation.

Regular dictionaries can be used instead of SON objects, but not when the order
of keys is important. A SON object can be used just like a normal Python
dictionary."""

import copy
import re

from bson.py3compat import abc, iteritems


# This sort of sucks, but seems to be as good as it gets...
# This is essentially the same as re._pattern_type
RE_TYPE = type(re.compile(""))


class SON(dict):
    """SON data.

    A subclass of dict that maintains ordering of keys and provides a
    few extra niceties for dealing with SON. SON provides an API
    similar to collections.OrderedDict from Python 2.7+.
    """

    def __init__(self, data=None, **kwargs):
        self.__keys = []
        dict.__init__(self)
        self.update(data)
        self.update(kwargs)

    def __new__(cls, *args, **kwargs):
        instance = super(SON, cls).__new__(cls, *args, **kwargs)
        instance.__keys = []
        return instance

    def __repr__(self):
        result = []
        for key in self.__keys:
            result.append("(%r, %r)" % (key, self[key]))
        return "SON([%s])" % ", ".join(result)

    def __setitem__(self, key, value):
        if key not in self.__keys:
            self.__keys.append(key)
        dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        self.__keys.remove(key)
        dict.__delitem__(self, key)

    def keys(self):
        return list(self.__keys)

    def copy(self):
        other = SON()
        other.update(self)
        return other

    # TODO this is all from UserDict.DictMixin. it could probably be made more
    # efficient.
    # second level definitions support higher levels
    def __iter__(self):
        for k in self.__keys:
            yield k

    def has_key(self, key):
        return key in self.__keys

    # third level takes advantage of second level definitions
    def iteritems(self):
        for k in self:
            yield (k, self[k])

    def iterkeys(self):
        return self.__iter__()

    # fourth level uses definitions from lower levels
    def itervalues(self):
        for _, v in self.iteritems():
            yield v

    def values(self):
        return [v for _, v in self.iteritems()]

    def items(self):
        return [(key, self[key]) for key in self]

    def clear(self):
        self.__keys = []
        super(SON, self).clear()

    def setdefault(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            self[key] = default
        return default

    def pop(self, key, *args):
        if len(args) > 1:
            raise TypeError("pop expected at most 2 arguments, got "\
                                + repr(1 + len(args)))
        try:
            value = self[key]
        except KeyError:
            if args:
                return args[0]
            raise
        del self[key]
        return value

    def popitem(self):
        try:
            k, v = next(self.iteritems())
        except StopIteration:
            raise KeyError('container is empty')
        del self[k]
        return (k, v)

    def update(self, other=None, **kwargs):
        # Make progressively weaker assumptions about "other"
        if other is None:
            pass
        elif hasattr(other, 'iteritems'):  # iteritems saves memory and lookups
            for k, v in other.iteritems():
                self[k] = v
        elif hasattr(other, 'keys'):
            for k in other.keys():
                self[k] = other[k]
        else:
            for k, v in other:
                self[k] = v
        if kwargs:
            self.update(kwargs)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __eq__(self, other):
        """Comparison to another SON is order-sensitive while comparison to a
        regular dictionary is order-insensitive.
        """
        if isinstance(other, SON):
            return len(self) == len(other) and self.items() == other.items()
        return self.to_dict() == other

    def __ne__(self, other):
        return not self == other

    def __len__(self):
        return len(self.__keys)

    def to_dict(self):
        """Convert a SON document to a normal Python dictionary instance.

        This is trickier than just *dict(...)* because it needs to be
        recursive.
        """

        def transform_value(value):
            if isinstance(value, list):
                return [transform_value(v) for v in value]
            elif isinstance(value, abc.Mapping):
                return dict([
                    (k, transform_value(v))
                    for k, v in iteritems(value)])
            else:
                return value

        return transform_value(dict(self))

    def __deepcopy__(self, memo):
        out = SON()
        val_id = id(self)
        if val_id in memo:
            return memo.get(val_id)
        memo[val_id] = out
        for k, v in self.iteritems():
            if not isinstance(v, RE_TYPE):
                v = copy.deepcopy(v, memo)
            out[k] = v
        return out
