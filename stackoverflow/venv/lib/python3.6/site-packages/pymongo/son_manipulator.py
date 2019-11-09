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

"""**DEPRECATED**: Manipulators that can edit SON objects as they enter and exit
a database.

The :class:`~pymongo.son_manipulator.SONManipulator` API has limitations as a
technique for transforming your data. Instead, it is more flexible and
straightforward to transform outgoing documents in your own code before passing
them to PyMongo, and transform incoming documents after receiving them from
PyMongo. SON Manipulators will be removed from PyMongo in 4.0.

PyMongo does **not** apply SON manipulators to documents passed to
the modern methods :meth:`~pymongo.collection.Collection.bulk_write`,
:meth:`~pymongo.collection.Collection.insert_one`,
:meth:`~pymongo.collection.Collection.insert_many`,
:meth:`~pymongo.collection.Collection.update_one`, or
:meth:`~pymongo.collection.Collection.update_many`. SON manipulators are
**not** applied to documents returned by the modern methods
:meth:`~pymongo.collection.Collection.find_one_and_delete`,
:meth:`~pymongo.collection.Collection.find_one_and_replace`, and
:meth:`~pymongo.collection.Collection.find_one_and_update`.
"""

from bson.dbref import DBRef
from bson.objectid import ObjectId
from bson.py3compat import abc
from bson.son import SON


class SONManipulator(object):
    """A base son manipulator.

    This manipulator just saves and restores objects without changing them.
    """

    def will_copy(self):
        """Will this SON manipulator make a copy of the incoming document?

        Derived classes that do need to make a copy should override this
        method, returning True instead of False. All non-copying manipulators
        will be applied first (so that the user's document will be updated
        appropriately), followed by copying manipulators.
        """
        return False

    def transform_incoming(self, son, collection):
        """Manipulate an incoming SON object.

        :Parameters:
          - `son`: the SON object to be inserted into the database
          - `collection`: the collection the object is being inserted into
        """
        if self.will_copy():
            return SON(son)
        return son

    def transform_outgoing(self, son, collection):
        """Manipulate an outgoing SON object.

        :Parameters:
          - `son`: the SON object being retrieved from the database
          - `collection`: the collection this object was stored in
        """
        if self.will_copy():
            return SON(son)
        return son


class ObjectIdInjector(SONManipulator):
    """A son manipulator that adds the _id field if it is missing.

    .. versionchanged:: 2.7
       ObjectIdInjector is no longer used by PyMongo, but remains in this
       module for backwards compatibility.
    """

    def transform_incoming(self, son, collection):
        """Add an _id field if it is missing.
        """
        if not "_id" in son:
            son["_id"] = ObjectId()
        return son


# This is now handled during BSON encoding (for performance reasons),
# but I'm keeping this here as a reference for those implementing new
# SONManipulators.
class ObjectIdShuffler(SONManipulator):
    """A son manipulator that moves _id to the first position.
    """

    def will_copy(self):
        """We need to copy to be sure that we are dealing with SON, not a dict.
        """
        return True

    def transform_incoming(self, son, collection):
        """Move _id to the front if it's there.
        """
        if not "_id" in son:
            return son
        transformed = SON({"_id": son["_id"]})
        transformed.update(son)
        return transformed


class NamespaceInjector(SONManipulator):
    """A son manipulator that adds the _ns field.
    """

    def transform_incoming(self, son, collection):
        """Add the _ns field to the incoming object
        """
        son["_ns"] = collection.name
        return son


class AutoReference(SONManipulator):
    """Transparently reference and de-reference already saved embedded objects.

    This manipulator should probably only be used when the NamespaceInjector is
    also being used, otherwise it doesn't make too much sense - documents can
    only be auto-referenced if they have an *_ns* field.

    NOTE: this will behave poorly if you have a circular reference.

    TODO: this only works for documents that are in the same database. To fix
    this we'll need to add a DatabaseInjector that adds *_db* and then make
    use of the optional *database* support for DBRefs.
    """

    def __init__(self, db):
        self.database = db

    def will_copy(self):
        """We need to copy so the user's document doesn't get transformed refs.
        """
        return True

    def transform_incoming(self, son, collection):
        """Replace embedded documents with DBRefs.
        """

        def transform_value(value):
            if isinstance(value, abc.MutableMapping):
                if "_id" in value and "_ns" in value:
                    return DBRef(value["_ns"], transform_value(value["_id"]))
                else:
                    return transform_dict(SON(value))
            elif isinstance(value, list):
                return [transform_value(v) for v in value]
            return value

        def transform_dict(object):
            for (key, value) in object.items():
                object[key] = transform_value(value)
            return object

        return transform_dict(SON(son))

    def transform_outgoing(self, son, collection):
        """Replace DBRefs with embedded documents.
        """

        def transform_value(value):
            if isinstance(value, DBRef):
                return self.database.dereference(value)
            elif isinstance(value, list):
                return [transform_value(v) for v in value]
            elif isinstance(value, abc.MutableMapping):
                return transform_dict(SON(value))
            return value

        def transform_dict(object):
            for (key, value) in object.items():
                object[key] = transform_value(value)
            return object

        return transform_dict(SON(son))
