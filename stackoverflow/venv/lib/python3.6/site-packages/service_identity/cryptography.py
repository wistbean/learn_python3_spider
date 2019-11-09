"""
`cryptography.x509 <https://github.com/pyca/cryptography>`_-specific code.
"""

from __future__ import absolute_import, division, print_function

import warnings

from cryptography.x509 import (
    DNSName,
    ExtensionOID,
    IPAddress,
    NameOID,
    ObjectIdentifier,
    OtherName,
    UniformResourceIdentifier,
)
from cryptography.x509.extensions import ExtensionNotFound
from pyasn1.codec.der.decoder import decode
from pyasn1.type.char import IA5String

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


__all__ = ["verify_certificate_hostname"]


def verify_certificate_hostname(certificate, hostname):
    """
    Verify whether *certificate* is valid for *hostname*.

    .. note:: Nothing is verified about the *authority* of the certificate;
       the caller must verify that the certificate chains to an appropriate
       trust root themselves.

    :param cryptography.x509.Certificate certificate: A cryptography X509
        certificate object.
    :param unicode hostname: The hostname that *certificate* should be valid
        for.

    :raises service_identity.VerificationError: If *certificate* is not valid
        for *hostname*.
    :raises service_identity.CertificateError: If *certificate* contains
        invalid/unexpected data.

    :returns: ``None``
    """
    verify_service_identity(
        cert_patterns=extract_ids(certificate),
        obligatory_ids=[DNS_ID(hostname)],
        optional_ids=[],
    )


def verify_certificate_ip_address(certificate, ip_address):
    """
    Verify whether *certificate* is valid for *ip_address*.

    .. note:: Nothing is verified about the *authority* of the certificate;
       the caller must verify that the certificate chains to an appropriate
       trust root themselves.

    :param cryptography.x509.Certificate certificate: A cryptography X509
        certificate object.
    :param unicode ip_address: The IP address that *connection* should be valid
        for.  Can be an IPv4 or IPv6 address.

    :raises service_identity.VerificationError: If *certificate* is not valid
        for *ip_address*.
    :raises service_identity.CertificateError: If *certificate* contains
        invalid/unexpected data.

    :returns: ``None``

    .. versionadded:: 18.1.0
    """
    verify_service_identity(
        cert_patterns=extract_ids(certificate),
        obligatory_ids=[IPAddress_ID(ip_address)],
        optional_ids=[],
    )


ID_ON_DNS_SRV = ObjectIdentifier("1.3.6.1.5.5.7.8.7")  # id_on_dnsSRV


def extract_ids(cert):
    """
    Extract all valid IDs from a certificate for service verification.

    If *cert* doesn't contain any identifiers, the ``CN``s are used as DNS-IDs
    as fallback.

    :param cryptography.x509.Certificate cert: The certificate to be dissected.

    :return: List of IDs.
    """
    ids = []
    try:
        ext = cert.extensions.get_extension_for_oid(
            ExtensionOID.SUBJECT_ALTERNATIVE_NAME
        )
    except ExtensionNotFound:
        pass
    else:
        ids.extend(
            [
                DNSPattern(name.encode("utf-8"))
                for name in ext.value.get_values_for_type(DNSName)
            ]
        )
        ids.extend(
            [
                URIPattern(uri.encode("utf-8"))
                for uri in ext.value.get_values_for_type(
                    UniformResourceIdentifier
                )
            ]
        )
        ids.extend(
            [
                IPAddressPattern(ip)
                for ip in ext.value.get_values_for_type(IPAddress)
            ]
        )
        for other in ext.value.get_values_for_type(OtherName):
            if other.type_id == ID_ON_DNS_SRV:
                srv, _ = decode(other.value)
                if isinstance(srv, IA5String):
                    ids.append(SRVPattern(srv.asOctets()))
                else:  # pragma: nocover
                    raise CertificateError("Unexpected certificate content.")

    if not ids:
        # https://tools.ietf.org/search/rfc6125#section-6.4.4
        # A client MUST NOT seek a match for a reference identifier of CN-ID if
        # the presented identifiers include a DNS-ID, SRV-ID, URI-ID, or any
        # application-specific identifier types supported by the client.
        cns = [
            n.value
            for n in cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
        ]
        cn = next(iter(cns), b"<not given>")
        ids = [DNSPattern(n.encode("utf-8")) for n in cns]
        warnings.warn(
            "Certificate with CN {!r} has no `subjectAltName`, falling back "
            "to check for a `commonName` for now.  This feature is being "
            "removed by major browsers and deprecated by RFC 2818.".format(cn),
            SubjectAltNameWarning,
        )
    return ids
