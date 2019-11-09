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

"""Python driver for MongoDB."""

ASCENDING = 1
"""Ascending sort order."""
DESCENDING = -1
"""Descending sort order."""

GEO2D = "2d"
"""Index specifier for a 2-dimensional `geospatial index`_.

.. _geospatial index: http://docs.mongodb.org/manual/core/2d/
"""

GEOHAYSTACK = "geoHaystack"
"""Index specifier for a 2-dimensional `haystack index`_.

.. versionadded:: 2.1

.. _haystack index: http://docs.mongodb.org/manual/core/geohaystack/
"""

GEOSPHERE = "2dsphere"
"""Index specifier for a `spherical geospatial index`_.

.. versionadded:: 2.5

.. _spherical geospatial index: http://docs.mongodb.org/manual/core/2dsphere/
"""

HASHED = "hashed"
"""Index specifier for a `hashed index`_.

.. versionadded:: 2.5

.. _hashed index: http://docs.mongodb.org/manual/core/index-hashed/
"""

TEXT = "text"
"""Index specifier for a `text index`_.

.. versionadded:: 2.7.1

.. _text index: http://docs.mongodb.org/manual/core/index-text/
"""

OFF = 0
"""No database profiling."""
SLOW_ONLY = 1
"""Only profile slow operations."""
ALL = 2
"""Profile all operations."""

version_tuple = (3, 9, 0)

def get_version_string():
    if isinstance(version_tuple[-1], str):
        return '.'.join(map(str, version_tuple[:-1])) + version_tuple[-1]
    return '.'.join(map(str, version_tuple))

__version__ = version = get_version_string()
"""Current version of PyMongo."""

from pymongo.collection import ReturnDocument
from pymongo.common import (MIN_SUPPORTED_WIRE_VERSION,
                            MAX_SUPPORTED_WIRE_VERSION)
from pymongo.cursor import CursorType
from pymongo.mongo_client import MongoClient
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.operations import (IndexModel,
                                InsertOne,
                                DeleteOne,
                                DeleteMany,
                                UpdateOne,
                                UpdateMany,
                                ReplaceOne)
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern

def has_c():
    """Is the C extension installed?"""
    try:
        from pymongo import _cmessage
        return True
    except ImportError:
        return False
