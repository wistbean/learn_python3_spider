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

"""Result class definitions."""

from pymongo.errors import InvalidOperation


class _WriteResult(object):
    """Base class for write result classes."""

    __slots__ = ("__acknowledged",)

    def __init__(self, acknowledged):
        self.__acknowledged = acknowledged

    def _raise_if_unacknowledged(self, property_name):
        """Raise an exception on property access if unacknowledged."""
        if not self.__acknowledged:
            raise InvalidOperation("A value for %s is not available when "
                                   "the write is unacknowledged. Check the "
                                   "acknowledged attribute to avoid this "
                                   "error." % (property_name,))

    @property
    def acknowledged(self):
        """Is this the result of an acknowledged write operation?

        The :attr:`acknowledged` attribute will be ``False`` when using
        ``WriteConcern(w=0)``, otherwise ``True``.

        .. note::
          If the :attr:`acknowledged` attribute is ``False`` all other
          attibutes of this class will raise
          :class:`~pymongo.errors.InvalidOperation` when accessed. Values for
          other attributes cannot be determined if the write operation was
          unacknowledged.

        .. seealso::
          :class:`~pymongo.write_concern.WriteConcern`
        """
        return self.__acknowledged


class InsertOneResult(_WriteResult):
    """The return type for :meth:`~pymongo.collection.Collection.insert_one`.
    """

    __slots__ = ("__inserted_id", "__acknowledged")

    def __init__(self, inserted_id, acknowledged):
        self.__inserted_id = inserted_id
        super(InsertOneResult, self).__init__(acknowledged)

    @property
    def inserted_id(self):
        """The inserted document's _id."""
        return self.__inserted_id


class InsertManyResult(_WriteResult):
    """The return type for :meth:`~pymongo.collection.Collection.insert_many`.
    """

    __slots__ = ("__inserted_ids", "__acknowledged")

    def __init__(self, inserted_ids, acknowledged):
        self.__inserted_ids = inserted_ids
        super(InsertManyResult, self).__init__(acknowledged)

    @property
    def inserted_ids(self):
        """A list of _ids of the inserted documents, in the order provided.

        .. note:: If ``False`` is passed for the `ordered` parameter to
          :meth:`~pymongo.collection.Collection.insert_many` the server
          may have inserted the documents in a different order than what
          is presented here.
        """
        return self.__inserted_ids


class UpdateResult(_WriteResult):
    """The return type for :meth:`~pymongo.collection.Collection.update_one`,
    :meth:`~pymongo.collection.Collection.update_many`, and
    :meth:`~pymongo.collection.Collection.replace_one`.
    """

    __slots__ = ("__raw_result", "__acknowledged")

    def __init__(self, raw_result, acknowledged):
        self.__raw_result = raw_result
        super(UpdateResult, self).__init__(acknowledged)

    @property
    def raw_result(self):
        """The raw result document returned by the server."""
        return self.__raw_result

    @property
    def matched_count(self):
        """The number of documents matched for this update."""
        self._raise_if_unacknowledged("matched_count")
        if self.upserted_id is not None:
            return 0
        return self.__raw_result.get("n", 0)

    @property
    def modified_count(self):
        """The number of documents modified.

        .. note:: modified_count is only reported by MongoDB 2.6 and later.
          When connected to an earlier server version, or in certain mixed
          version sharding configurations, this attribute will be set to
          ``None``.
        """
        self._raise_if_unacknowledged("modified_count")
        return self.__raw_result.get("nModified")

    @property
    def upserted_id(self):
        """The _id of the inserted document if an upsert took place. Otherwise
        ``None``.
        """
        self._raise_if_unacknowledged("upserted_id")
        return self.__raw_result.get("upserted")


class DeleteResult(_WriteResult):
    """The return type for :meth:`~pymongo.collection.Collection.delete_one`
    and :meth:`~pymongo.collection.Collection.delete_many`"""

    __slots__ = ("__raw_result", "__acknowledged")

    def __init__(self, raw_result, acknowledged):
        self.__raw_result = raw_result
        super(DeleteResult, self).__init__(acknowledged)

    @property
    def raw_result(self):
        """The raw result document returned by the server."""
        return self.__raw_result

    @property
    def deleted_count(self):
        """The number of documents deleted."""
        self._raise_if_unacknowledged("deleted_count")
        return self.__raw_result.get("n", 0)


class BulkWriteResult(_WriteResult):
    """An object wrapper for bulk API write results."""

    __slots__ = ("__bulk_api_result", "__acknowledged")

    def __init__(self, bulk_api_result, acknowledged):
        """Create a BulkWriteResult instance.

        :Parameters:
          - `bulk_api_result`: A result dict from the bulk API
          - `acknowledged`: Was this write result acknowledged? If ``False``
            then all properties of this object will raise
            :exc:`~pymongo.errors.InvalidOperation`.
        """
        self.__bulk_api_result = bulk_api_result
        super(BulkWriteResult, self).__init__(acknowledged)

    @property
    def bulk_api_result(self):
        """The raw bulk API result."""
        return self.__bulk_api_result

    @property
    def inserted_count(self):
        """The number of documents inserted."""
        self._raise_if_unacknowledged("inserted_count")
        return self.__bulk_api_result.get("nInserted")

    @property
    def matched_count(self):
        """The number of documents matched for an update."""
        self._raise_if_unacknowledged("matched_count")
        return self.__bulk_api_result.get("nMatched")

    @property
    def modified_count(self):
        """The number of documents modified.

        .. note:: modified_count is only reported by MongoDB 2.6 and later.
          When connected to an earlier server version, or in certain mixed
          version sharding configurations, this attribute will be set to
          ``None``.
        """
        self._raise_if_unacknowledged("modified_count")
        return self.__bulk_api_result.get("nModified")

    @property
    def deleted_count(self):
        """The number of documents deleted."""
        self._raise_if_unacknowledged("deleted_count")
        return self.__bulk_api_result.get("nRemoved")

    @property
    def upserted_count(self):
        """The number of documents upserted."""
        self._raise_if_unacknowledged("upserted_count")
        return self.__bulk_api_result.get("nUpserted")

    @property
    def upserted_ids(self):
        """A map of operation index to the _id of the upserted document."""
        self._raise_if_unacknowledged("upserted_ids")
        if self.__bulk_api_result:
            return dict((upsert["index"], upsert["_id"])
                        for upsert in self.bulk_api_result["upserted"])
