#
# Copyright (C) 2017 Satoru SATOH <ssato @ redhat.com>
# License: MIT
#
"""
Parser for SQLite database files.

.. versionadded:: 0.7.99
   Added an experimental parser for SQLite database files.

- Format to support: SQLite database files sqlite3 module can process
- Requirements: sqlite3 in python standard library
- Development Status: 3 - Alpha
- Limitations: Currently, it only supports simple loading and dumping
- Special options:
  - isolation_level to set isolation level of the connection object
  - extensions to provide a list of SQLite extension file paths to load. It's
    not available if you're using python 2.6.
"""
from __future__ import absolute_import

import itertools
import logging
import os.path
import os
import sqlite3

import anyconfig.backend.base
import anyconfig.compat
import m9dicts


LOGGER = logging.getLogger(__name__)
ILEVELS = (None, "DEFERRED", "IMMEDIATE", "EXCLUSIVE")

ERR_NOT_IMPL = "SQLite backend cannot do that!"


def _set_options(conn, **options):
    """
    :param conn: :class:`sqlite3.Connection` object
    :param options: See the description of `load` function below.
    """
    if "isolation_level" in options and options["isolation_level"] in ILEVELS:
        conn.isolation_level = options["isolation_level"]

    if "extensions" in options:
        conn.enable_load_extension(True)
        for ext_path in options["extensions"]:
            conn.load_extension(ext_path)


def load(conn, to_container=dict, **options):
    """
    Load config data from given initialized :class:`sqlite3.Connection` object.

    :param conn: :class:`sqlite3.Connection` object
    :param to_container:
        Factory function to create a dict-like object to store data
    :param options:
        Options applied to :class:`sqlite3.Connection` object, `conn`.
        Allowed options are:
        - isolation_level: Set isolation level of the connection
        - extensions: Extensions to load

        See also for more details of the options,
        https://docs.python.org/2.7/library/sqlite3.html#connection-objects
        for example.

    :return: Dict-like object holding shell variables' definitions
    """
    _set_options(conn, **options)
    ret = to_container()
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")

    for tbl in cur.fetchall():
        tname = tbl[0]
        res = cur.execute("PRAGMA table_info('%s')" % tname).fetchall()
        keys = zip(*res)[1]

        ret[tname] = [to_container(list(itertools.izip_longest(keys, vals)))
                      for vals in cur.execute("SELECT * FROM %s" % tname)]

    return ret


def _sqlite_type(val):
    """
    :param val: Value to detect its type
    :return: Expression of type in SQLite such like 'INTEGER' and 'TEXT'

    >>> _sqlite_type(1092)
    'INTEGER'
    >>> _sqlite_type(1.0)
    'REAL'
    >>> _sqlite_type("xyz")
    'TEXT'
    """
    vtype = type(val)
    if vtype in anyconfig.compat.STR_TYPES:
        return "TEXT"
    elif vtype == int:
        return "INTEGER"
    elif vtype == float:
        return "REAL"
    else:
        return "BLOB"


def _dml_st_itr(rel, data, keys):
    """
    Generator to yield statement to insert values from `data` into tables.

    :param rel: Relation (table) name
    :param data: A list of data which must not be empty
    :param keys: Data key names
    :return: Generator to yield a tuple of (DML_statement, values_to_insert)
    """
    nkeys = len(keys)
    for items in data:  # items :: [(key, value)]
        # ..note:: Some reserved keywords must be single-quoted.
        vars = ["'%s'" % k for k, _v in items]
        if len(items) < nkeys:  # Some values are missing.
            holders = ", ".join(":%s" % n for n in vars)
            stmt = "INSERT INTO '%s' (%s) VALUES (%s)" % (rel, ", ".join(vars),
                                                          holders)
            ret = (stmt, dict(items))
        else:
            holders = ", ".join("?" for _ in range(nkeys))
            ret = ("INSERT INTO '%s' VALUES (%s)" % (rel, holders),
                   zip(*items)[1])

        yield ret


def dump(cnf, conn, **options):
    """
    Dump config `cnf`.

    :param cnf: Configuration data to dump
    :param conn: :class:`sqlite3.Connection` object
    :param options: See the description of `load` function below.
    """
    _set_options(conn, **options)
    cur = conn.cursor()
    for rel, data in m9dicts.convert_to(cnf, to_type=m9dicts.RELATIONS_TYPE):
        if not data:
            continue

        data = sorted(data, key=len, reverse=True)

        keys = zip(*data[0])[0]
        kts = itertools.izip(keys, (_sqlite_type(v) for _k, v in data[0]))
        stmt = ("CREATE TABLE IF NOT EXISTS "
                "'%s' (%s)" % (rel, ", ".join("'%s' %s" % kt for kt in kts)))
        cur.execute(stmt)
        conn.commit()

        for stmt, items in _dml_st_itr(rel, data, keys):
            cur.execute(stmt, items)
        conn.commit()


def dumps(cnf, **options):
    """
    :param cnf: Configuration data to dump
    :param options: See the description of `load` function below.
    :return:
        SQL statements to dump config `cnf` as a string
    """
    with sqlite3.connect(":memory:") as conn:
        dump(cnf, conn, **options)
        return "\n".join(conn.iterdump())


def _fun(*args, **kwargs):
    """
    .. note::
       I think that both methods to load from string/dump to string do not make
       sense in the sqlite backend.
    """
    raise NotImplementedError(ERR_NOT_IMPL)


def _assert_conn(conn):
    """
    Custom assert to check sqlite3.Connection object.

    :param conn: :class:`sqlite3.Connection` object
    """
    if not isinstance(conn, sqlite3.Connection):
        raise RuntimeError("Given one is not a sqlite3.Connection object!")


class Parser(anyconfig.backend.base.FromStreamLoader,
             anyconfig.backend.base.ToStreamDumper):
    """
    Parser for SQLite database files.
    """
    _type = "sqlite"

    # Others like "timeout", "detect_types", "factory" are not supported yet.
    _load_opts = ["isolation_level", "extensions"]
    _dump_opts = []

    load_from_string = anyconfig.backend.base.to_method(_fun)

    @classmethod
    def ropen(cls, filepath, **kwargs):
        """
        :param filepath: Path to file to open to read data
        """
        try:
            uri = "file://%s?mode=ro" % os.path.abspath(filepath)
            conn = sqlite3.connect(uri, uri=True, **kwargs)  # python >= 3.4
        except sqlite3.OperationalError:
            with os.open(filepath, os.O_RDONLY) as fdi:  # Most *Nix env..
                conn = sqlite3.connect("/dev/fd/%d" % fdi, **kwargs)
        except:
            conn = sqlite3.connect(filepath, **kwargs)

        return conn

    @classmethod
    def wopen(cls, filepath, **kwargs):
        """
        :param filepath: Path to file to open to write data to
        """
        return sqlite3.connect(filepath, **kwargs)

    def load_from_stream(self, conn, to_container, **kwargs):
        """
        Load config from :class:`sqlite3.Connection` object

        :param conn: :class:`sqlite3.Connection` object
        :param to_container: callble to make a container object later
        :param kwargs: optional keyword parameters to be sanitized
        :param options:
            options will be passed to backend specific loading functions.

        :return: dict or dict-like object holding configurations
        """
        _assert_conn(conn)
        return load(conn, to_container, **kwargs)

    def dump_to_stream(self, cnf, conn, **kwargs):
        """
        Dump config `cnf` from initialized :class:`sqlite3.Connection` object.

        :param cnf: Config data to dump
        :param conn: :class:`sqlite3.Connection` object
        :param kwargs: optional keyword parameters to be sanitized
        """
        _assert_conn(conn)
        dump(cnf, conn, **kwargs)

    def dump_to_string(self, cnf, **kwargs):
        """
        Dump config `cnf` to a string.

        :param cnf: Configuration data to dump
        :param kwargs: optional keyword parameters to be sanitized :: dict

        :return: Dict-like object holding config parameters
        """
        return dumps(cnf, **kwargs)

# vim:sw=4:ts=4:et:
