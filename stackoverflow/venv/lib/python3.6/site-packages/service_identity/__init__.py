"""
Verify service identities.
"""

from __future__ import absolute_import, division, print_function

from . import cryptography, pyopenssl
from .exceptions import (
    CertificateError,
    SubjectAltNameWarning,
    VerificationError,
)


__version__ = "18.1.0"

__title__ = "service_identity"
__description__ = "Service identity verification for pyOpenSSL & cryptography."
__uri__ = "https://service-identity.readthedocs.io/"

__author__ = "Hynek Schlawack"
__email__ = "hs@ox.cx"

__license__ = "MIT"
__copyright__ = "Copyright (c) 2014 Hynek Schlawack"


__all__ = [
    "CertificateError",
    "SubjectAltNameWarning",
    "VerificationError",
    "cryptography",
    "pyopenssl",
]
