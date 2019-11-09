# Copyright 2018-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Advanced options for MongoDB drivers implemented on top of PyMongo."""

from collections import namedtuple

from bson.py3compat import string_type


class DriverInfo(namedtuple('DriverInfo', ['name', 'version', 'platform'])):
    """Info about a driver wrapping PyMongo.

    The MongoDB server logs PyMongo's name, version, and platform whenever
    PyMongo establishes a connection. A driver implemented on top of PyMongo
    can add its own info to this log message. Initialize with three strings
    like 'MyDriver', '1.2.3', 'some platform info'. Any of these strings may be
    None to accept PyMongo's default.
    """
    def __new__(cls, name=None, version=None, platform=None):
        self = super(DriverInfo, cls).__new__(cls, name, version, platform)
        for name, value in self._asdict().items():
            if value is not None and not isinstance(value, string_type):
                raise TypeError("Wrong type for DriverInfo %s option, value "
                                "must be an instance of %s" % (
                                    name, string_type.__name__))

        return self
