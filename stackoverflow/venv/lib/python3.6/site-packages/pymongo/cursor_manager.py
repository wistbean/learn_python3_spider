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

"""DEPRECATED - A manager to handle when cursors are killed after they are
closed.

New cursor managers should be defined as subclasses of CursorManager and can be
installed on a client by calling
:meth:`~pymongo.mongo_client.MongoClient.set_cursor_manager`.

.. versionchanged:: 3.3
   Deprecated, for real this time.

.. versionchanged:: 3.0
   Undeprecated. :meth:`~pymongo.cursor_manager.CursorManager.close` now
   requires an `address` argument. The ``BatchCursorManager`` class is removed.
"""

import warnings
import weakref
from bson.py3compat import integer_types


class CursorManager(object):
    """DEPRECATED - The cursor manager base class."""

    def __init__(self, client):
        """Instantiate the manager.

        :Parameters:
          - `client`: a MongoClient
        """
        warnings.warn(
            "Cursor managers are deprecated.",
            DeprecationWarning,
            stacklevel=2)
        self.__client = weakref.ref(client)

    def close(self, cursor_id, address):
        """Kill a cursor.

        Raises TypeError if cursor_id is not an instance of (int, long).

        :Parameters:
          - `cursor_id`: cursor id to close
          - `address`: the cursor's server's (host, port) pair

        .. versionchanged:: 3.0
           Now requires an `address` argument.
        """
        if not isinstance(cursor_id, integer_types):
            raise TypeError("cursor_id must be an integer")

        self.__client().kill_cursors([cursor_id], address)
