#
# Copyright (C) 2017 Satoru SATOH <ssato @ redhat.com>
# License: MIT
#
# pylint: disable=missing-docstring
import anyconfig.backend.sqlite as TT
import anyconfig.backend.tests.ini


CNF_0_S = """
...
"""

CNF_0 = dict(A=[dict(a=1, b="b", id=0)])

CNF_0_S_OUT = """\
BEGIN TRANSACTION;
CREATE TABLE 'A' ('A' INTEGER,
'id' INTEGER, FOREIGN KEY(A) REFERENCES A_A(id));
INSERT INTO "A" VALUES('A','id');
CREATE TABLE 'A_A' ('a' INTEGER,
'b' TEXT,
'id' INTEGER);
INSERT INTO "A_A" VALUES('a','b','id');
COMMIT;\
"""


class Test10(anyconfig.backend.tests.ini.Test10):

    cnf = CNF_0
    cnf_s = CNF_0_S
    cnf_s_out = CNF_0_S_OUT

    load_options = dict(isolation_level=None)
    is_order_kept = False

    def setUp(self):
        self.psr = TT.Parser()

    def test_10_loads(self):
        pass  # TODO

    def test_12_loads__w_options(self):
        pass  # TODO

    def test_30_loads_with_order_kept(self):
        pass  # TODO

    def test_20_dumps(self):
        cnf_s = self.psr.dumps(self.cnf)
        self.assertEqual(cnf_s, self.cnf_s_out)

    def test_22_dumps__w_options(self):
        cnf_s = self.psr.dumps(self.cnf, isolation_level=None)
        self.assertEqual(cnf_s, self.cnf_s_out)

# vim:sw=4:ts=4:et:
