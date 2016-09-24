#
# Copyright (C) 2016 Satoru SATOH <ssato @ redhat.com>
# License: MIT
#
# pylint: disable=missing-docstring
from __future__ import absolute_import

import subproccess
import unittest
import anyconfig.backend.tests.ini
import anyconfig.backend.winreg as TT

from anyconfig.tests.common import dicts_equal


class Test00(unittest.TestCase):

    def test_10_decode_0(self):


"""
class Test10(anyconfig.backend.tests.ini.Test10):

    cnf = CNF_0
    cnf_s = CNF_0_S
    load_options = dump_options = dict(parse_int=None, indent=3)

    if IS_PYTHON_2_6:
        is_order_kept = False  # ..note:: object_pairs_hoo is not available.

    def setUp(self):
        self.psr = TT.Parser()


class Test20(anyconfig.backend.tests.ini.Test20):

    psr_cls = TT.Parser
    cnf = CNF_0
    cnf_s = CNF_0_S
    cnf_fn = "conf0.json"

    def test_12_load__w_options(self):
        cnf = self.psr.load(self.cpath, parse_int=None)
        self.assertTrue(dicts_equal(cnf, self.cnf), str(cnf))

    def test_22_dump__w_special_option(self):
        self.psr.dump(self.cnf, self.cpath, parse_int=None, indent=3)
        cnf = self.psr.load(self.cpath)
        self.assertTrue(dicts_equal(cnf, self.cnf), str(cnf))
"""

# vim:sw=4:ts=4:et:
