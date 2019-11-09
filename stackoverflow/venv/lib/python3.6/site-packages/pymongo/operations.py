# Copyright 2015-present MongoDB, Inc.
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

"""Operation class definitions."""

from pymongo.common import validate_boolean, validate_is_mapping, validate_list
from pymongo.collation import validate_collation_or_none
from pymongo.helpers import _gen_index_name, _index_document, _index_list


class InsertOne(object):
    """Represents an insert_one operation."""

    __slots__ = ("_doc",)

    def __init__(self, document):
        """Create an InsertOne instance.

        For use with :meth:`~pymongo.collection.Collection.bulk_write`.

        :Parameters:
          - `document`: The document to insert. If the document is missing an
            _id field one will be added.
        """
        self._doc = document

    def _add_to_bulk(self, bulkobj):
        """Add this operation to the _Bulk instance `bulkobj`."""
        bulkobj.add_insert(self._doc)

    def __repr__(self):
        return "InsertOne(%r)" % (self._doc,)

    def __eq__(self, other):
        if type(other) == type(self):
            return other._doc == self._doc
        return NotImplemented

    def __ne__(self, other):
        return not self == other


class DeleteOne(object):
    """Represents a delete_one operation."""

    __slots__ = ("_filter", "_collation")

    def __init__(self, filter, collation=None):
        """Create a DeleteOne instance.

        For use with :meth:`~pymongo.collection.Collection.bulk_write`.

        :Parameters:
          - `filter`: A query that matches the document to delete.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only
            supported on MongoDB 3.4 and above.

        .. versionchanged:: 3.5
           Added the `collation` option.
        """
        if filter is not None:
            validate_is_mapping("filter", filter)
        self._filter = filter
        self._collation = collation

    def _add_to_bulk(self, bulkobj):
        """Add this operation to the _Bulk instance `bulkobj`."""
        bulkobj.add_delete(self._filter, 1, collation=self._collation)

    def __repr__(self):
        return "DeleteOne(%r, %r)" % (self._filter, self._collation)

    def __eq__(self, other):
        if type(other) == type(self):
            return ((other._filter, other._collation) ==
                    (self._filter, self._collation))
        return NotImplemented

    def __ne__(self, other):
        return not self == other


class DeleteMany(object):
    """Represents a delete_many operation."""

    __slots__ = ("_filter", "_collation")

    def __init__(self, filter, collation=None):
        """Create a DeleteMany instance.

        For use with :meth:`~pymongo.collection.Collection.bulk_write`.

        :Parameters:
          - `filter`: A query that matches the documents to delete.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only
            supported on MongoDB 3.4 and above.

        .. versionchanged:: 3.5
           Added the `collation` option.
        """
        if filter is not None:
            validate_is_mapping("filter", filter)
        self._filter = filter
        self._collation = collation

    def _add_to_bulk(self, bulkobj):
        """Add this operation to the _Bulk instance `bulkobj`."""
        bulkobj.add_delete(self._filter, 0, collation=self._collation)

    def __repr__(self):
        return "DeleteMany(%r, %r)" % (self._filter, self._collation)

    def __eq__(self, other):
        if type(other) == type(self):
            return ((other._filter, other._collation) ==
                    (self._filter, self._collation))
        return NotImplemented

    def __ne__(self, other):
        return not self == other


class ReplaceOne(object):
    """Represents a replace_one operation."""

    __slots__ = ("_filter", "_doc", "_upsert", "_collation")

    def __init__(self, filter, replacement, upsert=False, collation=None):
        """Create a ReplaceOne instance.

        For use with :meth:`~pymongo.collection.Collection.bulk_write`.

        :Parameters:
          - `filter`: A query that matches the document to replace.
          - `replacement`: The new document.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only
            supported on MongoDB 3.4 and above.

        .. versionchanged:: 3.5
           Added the `collation` option.
        """
        if filter is not None:
            validate_is_mapping("filter", filter)
        if upsert is not None:
            validate_boolean("upsert", upsert)
        self._filter = filter
        self._doc = replacement
        self._upsert = upsert
        self._collation = collation

    def _add_to_bulk(self, bulkobj):
        """Add this operation to the _Bulk instance `bulkobj`."""
        bulkobj.add_replace(self._filter, self._doc, self._upsert,
                            collation=self._collation)

    def __eq__(self, other):
        if type(other) == type(self):
            return (
                (other._filter, other._doc, other._upsert, other._collation) ==
                (self._filter, self._doc, self._upsert, self._collation))
        return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "%s(%r, %r, %r, %r)" % (
            self.__class__.__name__, self._filter, self._doc, self._upsert,
            self._collation)


class _UpdateOp(object):
    """Private base class for update operations."""

    __slots__ = ("_filter", "_doc", "_upsert", "_collation", "_array_filters")

    def __init__(self, filter, doc, upsert, collation, array_filters):
        if filter is not None:
            validate_is_mapping("filter", filter)
        if upsert is not None:
            validate_boolean("upsert", upsert)
        if array_filters is not None:
            validate_list("array_filters", array_filters)
        self._filter = filter
        self._doc = doc
        self._upsert = upsert
        self._collation = collation
        self._array_filters = array_filters

    def __eq__(self, other):
        if type(other) == type(self):
            return (
                (other._filter, other._doc, other._upsert, other._collation,
                 other._array_filters) ==
                (self._filter, self._doc, self._upsert, self._collation,
                 self._array_filters))
        return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "%s(%r, %r, %r, %r, %r)" % (
            self.__class__.__name__, self._filter, self._doc, self._upsert,
            self._collation, self._array_filters)


class UpdateOne(_UpdateOp):
    """Represents an update_one operation."""

    __slots__ = ()

    def __init__(self, filter, update, upsert=False, collation=None,
                 array_filters=None):
        """Represents an update_one operation.

        For use with :meth:`~pymongo.collection.Collection.bulk_write`.

        :Parameters:
          - `filter`: A query that matches the document to update.
          - `update`: The modifications to apply.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only
            supported on MongoDB 3.4 and above.
          - `array_filters` (optional): A list of filters specifying which
            array elements an update should apply. Requires MongoDB 3.6+.

        .. versionchanged:: 3.9
           Added the ability to accept a pipeline as the `update`.
        .. versionchanged:: 3.6
           Added the `array_filters` option.
        .. versionchanged:: 3.5
           Added the `collation` option.
        """
        super(UpdateOne, self).__init__(filter, update, upsert, collation,
                                        array_filters)

    def _add_to_bulk(self, bulkobj):
        """Add this operation to the _Bulk instance `bulkobj`."""
        bulkobj.add_update(self._filter, self._doc, False, self._upsert,
                           collation=self._collation,
                           array_filters=self._array_filters)


class UpdateMany(_UpdateOp):
    """Represents an update_many operation."""

    __slots__ = ()

    def __init__(self, filter, update, upsert=False, collation=None,
                 array_filters=None):
        """Create an UpdateMany instance.

        For use with :meth:`~pymongo.collection.Collection.bulk_write`.

        :Parameters:
          - `filter`: A query that matches the documents to update.
          - `update`: The modifications to apply.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`. This option is only
            supported on MongoDB 3.4 and above.
          - `array_filters` (optional): A list of filters specifying which
            array elements an update should apply. Requires MongoDB 3.6+.

        .. versionchanged:: 3.9
           Added the ability to accept a pipeline as the `update`.
        .. versionchanged:: 3.6
           Added the `array_filters` option.
        .. versionchanged:: 3.5
           Added the `collation` option.
        """
        super(UpdateMany, self).__init__(filter, update, upsert, collation,
                                         array_filters)

    def _add_to_bulk(self, bulkobj):
        """Add this operation to the _Bulk instance `bulkobj`."""
        bulkobj.add_update(self._filter, self._doc, True, self._upsert,
                           collation=self._collation,
                           array_filters=self._array_filters)


class IndexModel(object):
    """Represents an index to create."""

    __slots__ = ("__document",)

    def __init__(self, keys, **kwargs):
        """Create an Index instance.

        For use with :meth:`~pymongo.collection.Collection.create_indexes`.

        Takes either a single key or a list of (key, direction) pairs.
        The key(s) must be an instance of :class:`basestring`
        (:class:`str` in python 3), and the direction(s) must be one of
        (:data:`~pymongo.ASCENDING`, :data:`~pymongo.DESCENDING`,
        :data:`~pymongo.GEO2D`, :data:`~pymongo.GEOHAYSTACK`,
        :data:`~pymongo.GEOSPHERE`, :data:`~pymongo.HASHED`,
        :data:`~pymongo.TEXT`).

        Valid options include, but are not limited to:

          - `name`: custom name to use for this index - if none is
            given, a name will be generated.
          - `unique`: if ``True`` creates a uniqueness constraint on the index.
          - `background`: if ``True`` this index should be created in the
            background.
          - `sparse`: if ``True``, omit from the index any documents that lack
            the indexed field.
          - `bucketSize`: for use with geoHaystack indexes.
            Number of documents to group together within a certain proximity
            to a given longitude and latitude.
          - `min`: minimum value for keys in a :data:`~pymongo.GEO2D`
            index.
          - `max`: maximum value for keys in a :data:`~pymongo.GEO2D`
            index.
          - `expireAfterSeconds`: <int> Used to create an expiring (TTL)
            collection. MongoDB will automatically delete documents from
            this collection after <int> seconds. The indexed field must
            be a UTC datetime or the data will not expire.
          - `partialFilterExpression`: A document that specifies a filter for
            a partial index. Requires server version >= 3.2.
          - `collation`: An instance of :class:`~pymongo.collation.Collation`
            that specifies the collation to use in MongoDB >= 3.4.
          - `wildcardProjection`: Allows users to include or exclude specific
            field paths from a `wildcard index`_ using the { "$**" : 1} key
            pattern. Requires server version >= 4.2.

        See the MongoDB documentation for a full list of supported options by
        server version.

        :Parameters:
          - `keys`: a single key or a list of (key, direction)
            pairs specifying the index to create
          - `**kwargs` (optional): any additional index creation
            options (see the above list) should be passed as keyword
            arguments

        .. versionchanged:: 3.2
            Added partialFilterExpression to support partial indexes.

        .. _wildcard index: https://docs.mongodb.com/master/core/index-wildcard/#wildcard-index-core
        """
        keys = _index_list(keys)
        if "name" not in kwargs:
            kwargs["name"] = _gen_index_name(keys)
        kwargs["key"] = _index_document(keys)
        collation = validate_collation_or_none(kwargs.pop('collation', None))
        self.__document = kwargs
        if collation is not None:
            self.__document['collation'] = collation

    @property
    def document(self):
        """An index document suitable for passing to the createIndexes
        command.
        """
        return self.__document
