#
# Copyright (C) 2017 Satoru SATOH <ssato @ redhat.com>
# License: MIT
#
# pylint: disable=missing-docstring
import anyconfig.backend.sqlite as TT
import anyconfig.backend.tests.ini


class Test10(anyconfig.backend.tests.ini.Test10):

    load_options = dict(isolation_level=None)

    def setUp(self):
        self.psr = TT.Parser()

    def _loads_should_fail(self):
        try:
            return self.psr.loads(self.cnf_s)
        except NotImplementedError as exc:
            self.assertEqual(str(exc), TT.ERR_NOT_IMPL)

    def test_10_loads(self):
        self._loads_should_fail()

    def test_12_loads__w_options(self):
        self._loads_should_fail()

    def test_30_loads_with_order_kept(self):
        self._loads_should_fail()

    def test_20_dumps(self):
        pass  # TODO

    def test_22_dumps__w_options(self):
        pass  # TODO

# vim:sw=4:ts=4:et:
