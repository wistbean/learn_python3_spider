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

"""Representation for the MongoDB internal MinKey type.
"""


class MinKey(object):
    """MongoDB internal MinKey type.

    .. versionchanged:: 2.7
       ``MinKey`` now implements comparison operators.
    """

    _type_marker = 255

    def __eq__(self, other):
        return isinstance(other, MinKey)

    def __hash__(self):
        return hash(self._type_marker)

    def __ne__(self, other):
        return not self == other

    def __le__(self, dummy):
        return True

    def __lt__(self, other):
        return not isinstance(other, MinKey)

    def __ge__(self, other):
        return isinstance(other, MinKey)

    def __gt__(self, dummy):
        return False

    def __repr__(self):
        return "MinKey()"
