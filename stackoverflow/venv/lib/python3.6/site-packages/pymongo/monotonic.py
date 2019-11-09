# Copyright 2014-2015 MongoDB, Inc.
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

"""Time. Monotonic if possible.
"""

from __future__ import absolute_import

__all__ = ['time']

try:
    # Patches standard time module.
    # From https://pypi.python.org/pypi/Monotime.
    import monotime
except ImportError:
    pass

try:
    # From https://pypi.python.org/pypi/monotonic.
    from monotonic import monotonic as time
except ImportError:
    try:
        # Monotime or Python 3.
        from time import monotonic as time
    except ImportError:
        # Not monotonic.
        from time import time
