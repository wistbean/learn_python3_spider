# Copyright 2014-present MongoDB, Inc.
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

"""Represent a response from the server."""


class Response(object):
    __slots__ = ('_data', '_address', '_request_id', '_duration',
                 '_from_command', '_docs')

    def __init__(self, data, address, request_id, duration, from_command,
                 docs):
        """Represent a response from the server.

        :Parameters:
          - `data`: A network response message.
          - `address`: (host, port) of the source server.
          - `request_id`: The request id of this operation.
          - `duration`: The duration of the operation.
          - `from_command`: if the response is the result of a db command.
        """
        self._data = data
        self._address = address
        self._request_id = request_id
        self._duration = duration
        self._from_command = from_command
        self._docs = docs

    @property
    def data(self):
        """Server response's raw BSON bytes."""
        return self._data

    @property
    def address(self):
        """(host, port) of the source server."""
        return self._address

    @property
    def request_id(self):
        """The request id of this operation."""
        return self._request_id

    @property
    def duration(self):
        """The duration of the operation."""
        return self._duration

    @property
    def from_command(self):
        """If the response is a result from a db command."""
        return self._from_command

    @property
    def docs(self):
        """The decoded document(s)."""
        return self._docs

class ExhaustResponse(Response):
    __slots__ = ('_socket_info', '_pool')

    def __init__(self, data, address, socket_info, pool, request_id, duration,
                 from_command, docs):
        """Represent a response to an exhaust cursor's initial query.

        :Parameters:
          - `data`:  A network response message.
          - `address`: (host, port) of the source server.
          - `socket_info`: The SocketInfo used for the initial query.
          - `pool`: The Pool from which the SocketInfo came.
          - `request_id`: The request id of this operation.
          - `duration`: The duration of the operation.
          - `from_command`: If the response is the result of a db command.
        """
        super(ExhaustResponse, self).__init__(data,
                                              address,
                                              request_id,
                                              duration,
                                              from_command, docs)
        self._socket_info = socket_info
        self._pool = pool

    @property
    def socket_info(self):
        """The SocketInfo used for the initial query.

        The server will send batches on this socket, without waiting for
        getMores from the client, until the result set is exhausted or there
        is an error.
        """
        return self._socket_info

    @property
    def pool(self):
        """The Pool from which the SocketInfo came."""
        return self._pool
