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
- Limitations:
  - It only supports simple load and dump, and these are not symmetric
    operations, that is, data loaded and dumped may be changed from the
    original one.
  - API `loads` is not supported in this backend because
    :meth:`load_from_string` is not implemented.
  - This backend will generate some definitions of extra SQL tables and
    definitions of SQL tables resulted are not normalized and optimized at all.
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
import anyconfig.backend.relations
import anyconfig.compat

from anyconfig.compat import zip_ as zip


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


def _try_exec(cursor, sql, *args):
    """
    An wrapper function for :meth:`execute` of :class:`sqlite3.Cursor`.
    """
    excs = (sqlite3.IntegrityError, sqlite3.InterfaceError,
            sqlite3.InternalError)
    try:
        return cursor.execute(sql, *args)
    except excs as exc:
        LOGGER.error("exc=%s, sql=%s, args=%s", str(exc), sql, str(args))
        raise


def load(conn, to_container=dict, **options):
    """
    Load config data from given initialized :class:`sqlite3.Connection` object.

    .. todo:: Load from tables with foreign key constraints.

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

    :return: Dict-like object holding data came from SQLite database
    """
    _set_options(conn, **options)
    ret = to_container()
    cur = conn.cursor()
    tbls = _try_exec(cur,
                     "SELECT name,sql FROM sqlite_master WHERE type='table'")

    for tname in [t[0] for t in tbls]:
        keys = [x[1] for x
                in _try_exec(cur, "PRAGMA table_info('%s')" % tname)]
        ret[tname] = [to_container(list(itertools.izip_longest(keys, vals)))
                      for vals in _try_exec(cur, "SELECT * FROM %s" % tname)]

    return ret


def loads(stmts, to_container=dict, **options):
    """
    Load config data from given string reprensents SQL statements.

    :param stmts:
        A string represents SQL statements to create table and insert data
    :param to_container:
        Factory function to create a dict-like object to store data
    :param options:
        Options applied to :class:`sqlite3.Connection` object, `conn`.
        See also `load` defined in the above.

    :return: Dict-like object holding data came from SQLite database
    """
    conn = sqlite3.connect(":memory:")
    _set_options(conn, **options)
    cur = conn.cursor()
    cur.executescript(stmts)

    return load(conn, to_container=to_container)


def _is_ref(obj):
    """
    :return: True if given object `obj` is an instance of Ref.
    """
    return isinstance(obj, anyconfig.backend.relations.Ref)


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
    >>> ref = anyconfig.backend.relations.Ref(relvar="A.A", id=0)
    >>> _sqlite_type(ref)
    'INTEGER'
    """
    vtype = type(val)
    if vtype in anyconfig.compat.STR_TYPES:
        return "TEXT"
    elif vtype == int or _is_ref(val):
        return "INTEGER"
    elif vtype == float:
        return "REAL"
    else:
        return "BLOB"


def _sql_vars(items):
    """
    :param items:
        A list of pairs of key and value might contain :class:`Ref` object
    """
    return [v.id if _is_ref(v) else v for v in zip(*items)[1]]


def _dml_st_itr(rel, data, keys):
    """
    Generator to yield DML statement to insert values from `data` into tables.

    :param rel: Relation (table) name
    :param data: A list of data which must not be empty
    :param keys: Data key names
    :return: Generator to yield a tuple of (DML_statement, values_to_insert)
    """
    nkeys = len(keys)
    stmt = ("INSERT OR REPLACE INTO '%s' VALUES "
            "(%s)" % (rel, ", ".join('?' for _ in range(nkeys))))

    for items in data:  # items :: [(key, value)]
        # ..note:: Some reserved keywords must be single-quoted.
        if len(items) < nkeys:  # Some values are missing.
            rvars = _sql_vars(items)
            params = ("'%s' (%s)" % (rel,
                                     ", ".join("'%s'" % v for v in rvars)),
                      ", ".join('?' for _ in rvars))
            yield ("INSERT OR REPLACE INTO %s VALUES (%s)" % params,
                   zip(*items)[1])
        else:
            yield (stmt, _sql_vars(items))


def _create_table_and_insert_data(conn, relvar, data, foreign_keys=False):
    """
    :param conn: :class:`sqlite3.Connection` object
    :param relvar: Relation variable to be used as table name
    :param data: A list of data which must not be empty
    :param foreign_keys: True if `relvar` has foreign key constraints
    """
    # data = sorted(data, key=len, reverse=True)  # Is it needed?
    keys = zip(*data[0])[0]
    kts = ",\n".join("'%s' %s" % kt for kt
                     in zip(keys, (_sqlite_type(v) for _k, v in data[0])))
    if foreign_keys:
        fks = ",\n".join("FOREIGN KEY(%s) REFERENCES %s(id)" % (k, v.relvar)
                         for k, v in data[0] if _is_ref(v))
        stmt = "CREATE TABLE IF NOT EXISTS '%s' (%s, %s)" % (relvar, kts, fks)
    else:
        stmt = "CREATE TABLE IF NOT EXISTS '%s' (%s)" % (relvar, kts)

    cur = conn.cursor()
    _try_exec(cur, stmt)
    conn.commit()

    for stmt, items in _dml_st_itr(relvar, data, keys):
        _try_exec(cur, stmt, items)
    conn.commit()


def dump(cnf, conn, **options):
    """
    Dump config `cnf`.

    :param cnf: Configuration data to dump
    :param conn: :class:`sqlite3.Connection` object
    :param options: See the description of `load` function below.
    """
    _set_options(conn, **options)
    rels = [r for r in anyconfig.backend.relations.dict_to_rels(cnf) if r]
    rrels = []

    # 1. Create table of which does not have foreign key constraints.
    for relvar, data in rels:  # data :: [rel :: ((k0, v0), (k1, v1), ...)]
        if any(r for r in data if any(_is_ref(v) for v in zip(*r)[1])):
            rrels.append((relvar, data))
            continue

        _create_table_and_insert_data(conn, relvar, data)

    for relvar, data in rrels:
        _create_table_and_insert_data(conn, relvar, data, foreign_keys=True)


def dumps(cnf, **options):
    """
    :param cnf: Configuration data to dump
    :param options: See the description of `load` function below.
    :return:
        SQL statements to dump config `cnf` as a string

    >>> from __future__ import print_function
    >>> print(dumps(dict(a=1, b="b", c=[1, 2, 3]))
    ...      )  # doctest: +NORMALIZE_WHITESPACE
    BEGIN TRANSACTION;
    CREATE TABLE 'a_b_c' ('a' INTEGER,
    'b' TEXT,
    'id' INTEGER);
    INSERT INTO "a_b_c" VALUES(1,'b',920058062761);
    CREATE TABLE 'c' ('a_b_c' INTEGER,
    'c' INTEGER,
    'id' INTEGER, FOREIGN KEY(a_b_c) REFERENCES a_b_c(id));
    INSERT INTO "c" VALUES(920058062761,1,100085124795);
    INSERT INTO "c" VALUES(920058062761,2,227579964876);
    INSERT INTO "c" VALUES(920058062761,3,332005862713);
    COMMIT;
    >>>
    """
    with sqlite3.connect(":memory:") as conn:
        dump(cnf, conn, **options)
        return "\n".join(conn.iterdump())


def _assert_conn(conn):
    """
    Custom assert to check sqlite3.Connection object.

    :param conn: :class:`sqlite3.Connection` object
    """
    if not isinstance(conn, sqlite3.Connection):
        raise RuntimeError("Given one is not a sqlite3.Connection object!")


class Parser(anyconfig.backend.base.FromStreamLoader):
    """
    Parser for SQLite database files.
    """
    _type = "sqlite"

    # Others like "timeout", "detect_types", "factory" are not supported yet.
    _load_opts = ["isolation_level", "extensions"]
    _dump_opts = []

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

    def load_from_string(self, content, to_container, **kwargs):
        """
        Load config from given string `content` which represents SQL statements
        to create tables and insert data.

        :param content: Config content string gives SQL statements
        :param to_container: callble to make a container object later
        :param kwargs: optional keyword parameters to be sanitized :: dict

        :return: Dict-like object holding config parameters
        """
        return loads(content, to_container=to_container, **kwargs)

    def dump_to_stream(self, cnf, conn, **kwargs):
        """
        Dump config `cnf` from initialized :class:`sqlite3.Connection` object.

        :param cnf: Config data to dump
        :param conn: :class:`sqlite3.Connection` object
        :param kwargs: optional keyword parameters to be sanitized
        """
        _assert_conn(conn)
        dump(cnf, conn, **kwargs)

    def dump_to_path(self, cnf, filepath, **kwargs):
        """
        Dump config `cnf` to a file `filepath`.

        :param cnf: Config data to dump
        :param filepath: Config file path
        :param kwargs: optional keyword parameters to be sanitized
        """
        with self.wopen(filepath, **kwargs) as conn:
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
