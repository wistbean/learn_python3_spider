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

"""Tools for representing JavaScript code in BSON.
"""

from bson.py3compat import abc, string_type, PY3, text_type


class Code(str):
    """BSON's JavaScript code type.

    Raises :class:`TypeError` if `code` is not an instance of
    :class:`basestring` (:class:`str` in python 3) or `scope`
    is not ``None`` or an instance of :class:`dict`.

    Scope variables can be set by passing a dictionary as the `scope`
    argument or by using keyword arguments. If a variable is set as a
    keyword argument it will override any setting for that variable in
    the `scope` dictionary.

    :Parameters:
      - `code`: A string containing JavaScript code to be evaluated or another
        instance of Code. In the latter case, the scope of `code` becomes this
        Code's :attr:`scope`.
      - `scope` (optional): dictionary representing the scope in which
        `code` should be evaluated - a mapping from identifiers (as
        strings) to values. Defaults to ``None``. This is applied after any
        scope associated with a given `code` above.
      - `**kwargs` (optional): scope variables can also be passed as
        keyword arguments. These are applied after `scope` and `code`.

    .. versionchanged:: 3.4
      The default value for :attr:`scope` is ``None`` instead of ``{}``.

    """

    _type_marker = 13

    def __new__(cls, code, scope=None, **kwargs):
        if not isinstance(code, string_type):
            raise TypeError("code must be an "
                            "instance of %s" % (string_type.__name__))

        if not PY3 and isinstance(code, text_type):
            self = str.__new__(cls, code.encode('utf8'))
        else:
            self = str.__new__(cls, code)

        try:
            self.__scope = code.scope
        except AttributeError:
            self.__scope = None

        if scope is not None:
            if not isinstance(scope, abc.Mapping):
                raise TypeError("scope must be an instance of dict")
            if self.__scope is not None:
                self.__scope.update(scope)
            else:
                self.__scope = scope

        if kwargs:
            if self.__scope is not None:
                self.__scope.update(kwargs)
            else:
                self.__scope = kwargs

        return self

    @property
    def scope(self):
        """Scope dictionary for this instance or ``None``.
        """
        return self.__scope

    def __repr__(self):
        return "Code(%s, %r)" % (str.__repr__(self), self.__scope)

    def __eq__(self, other):
        if isinstance(other, Code):
            return (self.__scope, str(self)) == (other.__scope, str(other))
        return False

    __hash__ = None

    def __ne__(self, other):
        return not self == other
