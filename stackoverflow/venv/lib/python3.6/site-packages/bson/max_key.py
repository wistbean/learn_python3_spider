# Copyright 2010-present MongoDB, Inc.
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

"""Representation for the MongoDB internal MaxKey type.
"""


class MaxKey(object):
    """MongoDB internal MaxKey type.

    .. versionchanged:: 2.7
       ``MaxKey`` now implements comparison operators.
    """

    _type_marker = 127

    def __eq__(self, other):
        return isinstance(other, MaxKey)

    def __hash__(self):
        return hash(self._type_marker)

    def __ne__(self, other):
        return not self == other

    def __le__(self, other):
        return isinstance(other, MaxKey)

    def __lt__(self, dummy):
        return False

    def __ge__(self, dummy):
        return True

    def __gt__(self, other):
        return not isinstance(other, MaxKey)

    def __repr__(self):
        return "MaxKey()"
