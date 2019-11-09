"""
`pyOpenSSL <https://github.com/pyca/pyopenssl>`_-specific code.
"""

from __future__ import absolute_import, division, print_function

import warnings

import six

from pyasn1.codec.der.decoder import decode
from pyasn1.type.char import IA5String
from pyasn1.type.univ import ObjectIdentifier
from pyasn1_modules.rfc2459 import GeneralNames

from ._common import (
    DNS_ID,
    CertificateError,
    DNSPattern,
    IPAddress_ID,
    IPAddressPattern,
    SRVPattern,
    URIPattern,
    verify_service_identity,
)
from .exceptions import SubjectAltNameWarning


__all__ = ["verify_hostname"]


def verify_hostname(connection, hostname):
    """
    Verify whether the certificate of *connection* is valid for *hostname*.

    :param OpenSSL.SSL.Connection connection: A pyOpenSSL connection object.
    :param unicode hostname: The hostname that *connection* should be connected
        to.

    :raises service_identity.VerificationError: If *connection* does not
        provide a certificate that is valid for *hostname*.
    :raises service_identity.CertificateError: If the certificate chain of
        *connection* contains a certificate that contains invalid/unexpected
        data.

    :returns: ``None``
    """
    verify_service_identity(
        cert_patterns=extract_ids(connection.get_peer_certificate()),
        obligatory_ids=[DNS_ID(hostname)],
        optional_ids=[],
    )


def verify_ip_address(connection, ip_address):
    """
    Verify whether the certificate of *connection* is valid for *ip_address*.

    :param OpenSSL.SSL.Connection connection: A pyOpenSSL connection object.
    :param unicode ip_address: The IP address that *connection* should be
        connected to.  Can be an IPv4 or IPv6 address.

    :raises service_identity.VerificationError: If *connection* does not
        provide a certificate that is valid for *ip_address*.
    :raises service_identity.CertificateError: If the certificate chain of
        *connection* contains a certificate that contains invalid/unexpected
        data.

    :returns: ``None``

    .. versionadded:: 18.1.0
    """
    verify_service_identity(
        cert_patterns=extract_ids(connection.get_peer_certificate()),
        obligatory_ids=[IPAddress_ID(ip_address)],
        optional_ids=[],
    )


ID_ON_DNS_SRV = ObjectIdentifier("1.3.6.1.5.5.7.8.7")  # id_on_dnsSRV


def extract_ids(cert):
    """
    Extract all valid IDs from a certificate for service verification.

    If *cert* doesn't contain any identifiers, the ``CN``s are used as DNS-IDs
    as fallback.

    :param OpenSSL.SSL.X509 cert: The certificate to be dissected.

    :return: List of IDs.
    """
    ids = []
    for i in six.moves.range(cert.get_extension_count()):
        ext = cert.get_extension(i)
        if ext.get_short_name() == b"subjectAltName":
            names, _ = decode(ext.get_data(), asn1Spec=GeneralNames())
            for n in names:
                name_string = n.getName()
                if name_string == "dNSName":
                    ids.append(DNSPattern(n.getComponent().asOctets()))
                elif name_string == "iPAddress":
                    ids.append(
                        IPAddressPattern.from_bytes(
                            n.getComponent().asOctets()
                        )
                    )
                elif name_string == "uniformResourceIdentifier":
                    ids.append(URIPattern(n.getComponent().asOctets()))
                elif name_string == "otherName":
                    comp = n.getComponent()
                    oid = comp.getComponentByPosition(0)
                    if oid == ID_ON_DNS_SRV:
                        srv, _ = decode(comp.getComponentByPosition(1))
                        if isinstance(srv, IA5String):
                            ids.append(SRVPattern(srv.asOctets()))
                        else:  # pragma: nocover
                            raise CertificateError(
                                "Unexpected certificate content."
                            )
                    else:  # pragma: nocover
                        pass
                else:  # pragma: nocover
                    pass

    if not ids:
        # https://tools.ietf.org/search/rfc6125#section-6.4.4
        # A client MUST NOT seek a match for a reference identifier of CN-ID if
        # the presented identifiers include a DNS-ID, SRV-ID, URI-ID, or any
        # application-specific identifier types supported by the client.
        components = [
            c[1] for c in cert.get_subject().get_components() if c[0] == b"CN"
        ]
        cn = next(iter(components), b"<not given>")
        ids = [DNSPattern(c) for c in components]
        warnings.warn(
            "Certificate with CN '%s' has no `subjectAltName`, falling back "
            "to check for a `commonName` for now.  This feature is being "
            "removed by major browsers and deprecated by RFC 2818.  "
            "service_identity will remove the support for it in mid-2018."
            % (cn.decode("utf-8"),),
            SubjectAltNameWarning,
            stacklevel=2,
        )
    return ids
