#
# Copyright (C) 2016 Satoru SATOH <ssato @ redhat.com>
# License: MIT
#
#  pylint: disable=unused-argument
"""Windows(TM) Registry files backend.

.. versionadded:: 0.6.99

- Format to support: Windows registry,

  - https://support.microsoft.com/en-us/kb/256986
  - http://pogostick.net/~pnh/ntpasswd/WinReg.txt

- Requirements: python-registry, http://www.williballenthin.com/registry/
- Limitations:

  - Dump[s] APIs are not implemented / supported yet.

  - All values are encoded into base64 encoded and the output may be larger
    than original.

  - I'm not sure but maybe some data might be lost after load; I don't have
    hosts runing Windows and cannot test this module works as expected
    personally in actual.

- Special options: None
"""
from __future__ import absolute_import

import base64
import Registry.Registry as winreg
import anyconfig.backend.base


def _decode_0(rval, to_container=dict):
    """
    Decode :class:`Registry.Registry.RegistryValue` object.

    :param rval: :class:`Registry.Registry.RegistryValue` object
    :param to_container: any callable to make container

    :return: Dict or dict-like object represents this object
    """
    vtype = rval.value_type()
    if vtype == winreg.RegSZ:
        return to_container(mimetype="text/plain", value_type=vtype,
                            value=rval.value())
    else:
        return to_container(mimetype="application/octet-stream",
                            value_type=vtype,
                            value=base64.encodestring(rval.value()))


def _decode(rkey, to_container=dict):
    """
    Decode :class:`Registry.Registry.RegistryKey` object.

    :param rkey: :class:`Registry.Registry.RegistryKey` object
    :param to_container: any callable to make container

    :return: Dict or dict-like object represents this object
    """
    skeys = [_decode(sk, to_container) for sk in rkey.subkeys()]
    vals = [_decode_0(v, to_container) for v in rkey.values()]
    val = to_container(subkeys=skeys, values=vals,
                       timestamp=str(rkey.timestamp()))
    return to_container({rkey.path(): val})


class Parser(anyconfig.backend.base.FromStreamLoader):
    """
    Windows registry files parser.
    """
    _type = "registry"
    _open_flags = ('rb', 'wb')

    def load_from_stream(self, stream, to_container, **options):
        """
        Load config from given file like object `stream`.

        :param stream:  Config file or file like object
        :param to_container: callble to make a container object
        :param options: optional keyword arguments

        :return: Dict-like object holding config parameters
        """
        return _decode(winreg.Registry(stream).root(), to_container)

# vim:sw=4:ts=4:et:
