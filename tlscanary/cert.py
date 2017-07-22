#!/usr/bin/env python2

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from cryptography import x509
from cryptography.hazmat import backends
from cryptography.hazmat.primitives import serialization
from cryptography.x509.oid import ExtensionOID


class Cert(object):

    def __init__(self, data):
        if type(data) == list:
            data = ''.join([chr(x) for x in data])
        if type(data) != str:
            raise Exception("data must be string or list of uint8")
        self.__raw_data = data
        if "-----BEGIN CERTIFICATE-----" in data:
            self.x509 = x509.load_pem_x509_certificate(data, backends.default_backend())
            self.__raw_type = "PEM"
        else:
            self.x509 = x509.load_der_x509_certificate(data, backends.default_backend())
            self.__raw_type = "DER"

    def as_pem(self):
        return self.x509.public_bytes(encoding=serialization.Encoding.PEM)

    def as_der(self):
        return self.x509.public_bytes(encoding=serialization.Encoding.DER)

    def signature_hash_algorithm(self):
        return self.x509.signature_hash_algorithm.name

    def subject_alt_name(self):
        try:
            alt_names = self.x509.extensions.get_extension_for_oid(ExtensionOID.SUBJECT_ALTERNATIVE_NAME).value
            alt_name_strings = [alt_name.value for alt_name in alt_names]
            return ",".join(alt_name_strings)
        except x509.ExtensionNotFound:
            return "(no subject alt name)"

    def ext_key_usage(self):
        try:
            usages = self.x509.extensions.get_extension_for_oid(ExtensionOID.EXTENDED_KEY_USAGE).value
            usages_strings = [usage._name for usage in usages]
            return ",".join(usages_strings)
        except x509.ExtensionNotFound:
            return "(no ext key usage)"
