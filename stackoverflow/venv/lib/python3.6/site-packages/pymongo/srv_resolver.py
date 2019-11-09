# Copyright 2019-present MongoDB, Inc.
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

"""Support for resolving hosts and options from mongodb+srv:// URIs."""

try:
    from dns import resolver
    _HAVE_DNSPYTHON = True
except ImportError:
    _HAVE_DNSPYTHON = False

from bson.py3compat import PY3

from pymongo.common import CONNECT_TIMEOUT
from pymongo.errors import ConfigurationError


if PY3:
    # dnspython can return bytes or str from various parts
    # of its API depending on version. We always want str.
    def maybe_decode(text):
        if isinstance(text, bytes):
            return text.decode()
        return text
else:
    def maybe_decode(text):
        return text


class _SrvResolver(object):
    def __init__(self, fqdn, connect_timeout=None):
        self.__fqdn = fqdn
        self.__connect_timeout = connect_timeout or CONNECT_TIMEOUT

        # Validate the fully qualified domain name.
        try:
            self.__plist = self.__fqdn.split(".")[1:]
        except Exception:
            raise ConfigurationError("Invalid URI host: %s" % (fqdn,))
        self.__slen = len(self.__plist)
        if self.__slen < 2:
            raise ConfigurationError("Invalid URI host: %s" % (fqdn,))

    def get_options(self):
        try:
            results = resolver.query(self.__fqdn, 'TXT',
                                     lifetime=self.__connect_timeout)
        except (resolver.NoAnswer, resolver.NXDOMAIN):
            # No TXT records
            return None
        except Exception as exc:
            raise ConfigurationError(str(exc))
        if len(results) > 1:
            raise ConfigurationError('Only one TXT record is supported')
        return (
            b'&'.join([b''.join(res.strings) for res in results])).decode(
            'utf-8')

    def _resolve_uri(self, encapsulate_errors):
        try:
            results = resolver.query('_mongodb._tcp.' + self.__fqdn, 'SRV',
                                     lifetime=self.__connect_timeout)
        except Exception as exc:
            if not encapsulate_errors:
                # Raise the original error.
                raise
            # Else, raise all errors as ConfigurationError.
            raise ConfigurationError(str(exc))
        return results

    def _get_srv_response_and_hosts(self, encapsulate_errors):
        results = self._resolve_uri(encapsulate_errors)

        # Construct address tuples
        nodes = [
            (maybe_decode(res.target.to_text(omit_final_dot=True)), res.port)
            for res in results]

        # Validate hosts
        for node in nodes:
            try:
                nlist = node[0].split(".")[1:][-self.__slen:]
            except Exception:
                raise ConfigurationError("Invalid SRV host: %s" % (node[0],))
            if self.__plist != nlist:
                raise ConfigurationError("Invalid SRV host: %s" % (node[0],))

        return results, nodes

    def get_hosts(self):
        _, nodes = self._get_srv_response_and_hosts(True)
        return nodes

    def get_hosts_and_min_ttl(self):
        results, nodes = self._get_srv_response_and_hosts(False)
        return nodes, results.rrset.ttl
