# Copyright 2016 MongoDB, Inc.
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

"""Tools for working with `collations`_.

.. _collations: http://userguide.icu-project.org/collation/concepts
"""

from pymongo import common


class CollationStrength(object):
    """
    An enum that defines values for `strength` on a
    :class:`~pymongo.collation.Collation`.
    """

    PRIMARY = 1
    """Differentiate base (unadorned) characters."""

    SECONDARY = 2
    """Differentiate character accents."""

    TERTIARY = 3
    """Differentiate character case."""

    QUATERNARY = 4
    """Differentiate words with and without punctuation."""

    IDENTICAL = 5
    """Differentiate unicode code point (characters are exactly identical)."""


class CollationAlternate(object):
    """
    An enum that defines values for `alternate` on a
    :class:`~pymongo.collation.Collation`.
    """

    NON_IGNORABLE = 'non-ignorable'
    """Spaces and punctuation are treated as base characters."""

    SHIFTED = 'shifted'
    """Spaces and punctuation are *not* considered base characters.

    Spaces and punctuation are distinguished regardless when the
    :class:`~pymongo.collation.Collation` strength is at least
    :data:`~pymongo.collation.CollationStrength.QUATERNARY`.

    """


class CollationMaxVariable(object):
    """
    An enum that defines values for `max_variable` on a
    :class:`~pymongo.collation.Collation`.
    """

    PUNCT = 'punct'
    """Both punctuation and spaces are ignored."""

    SPACE = 'space'
    """Spaces alone are ignored."""


class CollationCaseFirst(object):
    """
    An enum that defines values for `case_first` on a
    :class:`~pymongo.collation.Collation`.
    """

    UPPER = 'upper'
    """Sort uppercase characters first."""

    LOWER = 'lower'
    """Sort lowercase characters first."""

    OFF = 'off'
    """Default for locale or collation strength."""


class Collation(object):
    """Collation

    :Parameters:
      - `locale`: (string) The locale of the collation. This should be a string
        that identifies an `ICU locale ID` exactly. For example, ``en_US`` is
        valid, but ``en_us`` and ``en-US`` are not. Consult the MongoDB
        documentation for a list of supported locales.
      - `caseLevel`: (optional) If ``True``, turn on case sensitivity if
        `strength` is 1 or 2 (case sensitivity is implied if `strength` is
        greater than 2). Defaults to ``False``.
      - `caseFirst`: (optional) Specify that either uppercase or lowercase
        characters take precedence. Must be one of the following values:

          * :data:`~CollationCaseFirst.UPPER`
          * :data:`~CollationCaseFirst.LOWER`
          * :data:`~CollationCaseFirst.OFF` (the default)

      - `strength`: (optional) Specify the comparison strength. This is also
        known as the ICU comparison level. This must be one of the following
        values:

          * :data:`~CollationStrength.PRIMARY`
          * :data:`~CollationStrength.SECONDARY`
          * :data:`~CollationStrength.TERTIARY` (the default)
          * :data:`~CollationStrength.QUATERNARY`
          * :data:`~CollationStrength.IDENTICAL`

        Each successive level builds upon the previous. For example, a
        `strength` of :data:`~CollationStrength.SECONDARY` differentiates
        characters based both on the unadorned base character and its accents.

      - `numericOrdering`: (optional) If ``True``, order numbers numerically
        instead of in collation order (defaults to ``False``).
      - `alternate`: (optional) Specify whether spaces and punctuation are
        considered base characters. This must be one of the following values:

          * :data:`~CollationAlternate.NON_IGNORABLE` (the default)
          * :data:`~CollationAlternate.SHIFTED`

      - `maxVariable`: (optional) When `alternate` is
        :data:`~CollationAlternate.SHIFTED`, this option specifies what
        characters may be ignored. This must be one of the following values:

          * :data:`~CollationMaxVariable.PUNCT` (the default)
          * :data:`~CollationMaxVariable.SPACE`

      - `normalization`: (optional) If ``True``, normalizes text into Unicode
        NFD. Defaults to ``False``.
      - `backwards`: (optional) If ``True``, accents on characters are
        considered from the back of the word to the front, as it is done in some
        French dictionary ordering traditions. Defaults to ``False``.
      - `kwargs`: (optional) Keyword arguments supplying any additional options
        to be sent with this Collation object.

    .. versionadded: 3.4

    """

    __slots__ = ("__document",)

    def __init__(self, locale,
                 caseLevel=None,
                 caseFirst=None,
                 strength=None,
                 numericOrdering=None,
                 alternate=None,
                 maxVariable=None,
                 normalization=None,
                 backwards=None,
                 **kwargs):
        locale = common.validate_string('locale', locale)
        self.__document = {'locale': locale}
        if caseLevel is not None:
            self.__document['caseLevel'] = common.validate_boolean(
                'caseLevel', caseLevel)
        if caseFirst is not None:
            self.__document['caseFirst'] = common.validate_string(
                'caseFirst', caseFirst)
        if strength is not None:
            self.__document['strength'] = common.validate_integer(
                'strength', strength)
        if numericOrdering is not None:
            self.__document['numericOrdering'] = common.validate_boolean(
                'numericOrdering', numericOrdering)
        if alternate is not None:
            self.__document['alternate'] = common.validate_string(
                'alternate', alternate)
        if maxVariable is not None:
            self.__document['maxVariable'] = common.validate_string(
                'maxVariable', maxVariable)
        if normalization is not None:
            self.__document['normalization'] = common.validate_boolean(
                'normalization', normalization)
        if backwards is not None:
            self.__document['backwards'] = common.validate_boolean(
                'backwards', backwards)
        self.__document.update(kwargs)

    @property
    def document(self):
        """The document representation of this collation.

        .. note::
          :class:`Collation` is immutable. Mutating the value of
          :attr:`document` does not mutate this :class:`Collation`.
        """
        return self.__document.copy()

    def __repr__(self):
        document = self.document
        return 'Collation(%s)' % (
            ', '.join('%s=%r' % (key, document[key]) for key in document),)

    def __eq__(self, other):
        if isinstance(other, Collation):
            return self.document == other.document
        return NotImplemented

    def __ne__(self, other):
        return not self == other


def validate_collation_or_none(value):
    if value is None:
        return None
    if isinstance(value, Collation):
        return value.document
    if isinstance(value, dict):
        return value
    raise TypeError(
        'collation must be a dict, an instance of collation.Collation, '
        'or None.')
