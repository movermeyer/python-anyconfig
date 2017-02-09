#
# Copyright (C) 2017 Red Hat, Inc.
# License: MIT
#
# Suppress warning about name of namedtuple objects:
# pylint: disable=invalid-name
"""Flatten nested dicts, etc.
"""
from __future__ import absolute_import

import collections
import itertools
import operator

import m9dicts.compat
import m9dicts.utils


Ref = collections.namedtuple("Ref", "relvar id".split())
ID_KEY = "id"


def object_to_id(obj, digits=12):
    """
    ..note:: Maybe collision occurs depends on `digits`

    :param obj: Any object has __str__ method to get its ID value
    :param digits: number of digists to take from generated hex digest str

    >>> object_to_id("test")
    127077368941
    >>> object_to_id({'a': "test"})
    262814736276
    >>> object_to_id(['a', 'b', 'c'])
    317209567448
    """
    nid = int(m9dicts.compat.md5(m9dicts.compat.to_str(obj)).hexdigest(), 16)
    return int(str(nid)[:digits])


def _gen_id(dic):
    """
    :return: ID (long int) value generated from `dic`
    """
    return object_to_id(sorted(dic.items()))


def _gen_relvar(*args):
    """
    :return: Generated ID string
    """
    return '_'.join(args)


def _rank_1_dict_to_rels(dic, relvar=None, idk=ID_KEY):
    """
    Convert a rank 1, not nested, dict to tuples of (relvar, tuples) where
    relvar represents the name of the relations and tuples are a list of tuples
    each represents a relation between key and value.

    :param dic: A dict or dict-like object maybe nested.
    :param relvar: Relation variable
    :param idk: Key to identify the item

    :return: A tuple of (<relvar>, [tuple of key and value])

    >>> _rank_1_dict_to_rels(dict(a=1, id=2), "data")
    ('data', (('a', 1), ('id', 2)))
    >>> _rank_1_dict_to_rels(dict(a=1, id=2))
    ('a_id', (('a', 1), ('id', 2)))
    >>> _rank_1_dict_to_rels(dict(a=1))  # doctest: +ELLIPSIS
    ('a', (('a', 1), ('id', 224491522528)))
    """
    if relvar is None:
        relvar = _gen_relvar(*sorted(dic.keys()))

    if idk not in dic:
        dic[idk] = _gen_id(dic)

    return (relvar, tuple(sorted((k, v) for k, v in dic.items())))


def _tuple_id(tpl, idk=ID_KEY):
    """
    Get the ID of given tuple `tpl`.

    :param tpl: (relvar, (tuple_0, ...))
    """
    try:
        tid = [t for t in tpl[1] if t[0] == idk][0][1]
        return (tpl[0], tid)  # (relvar, <id>)
    except IndexError:
        return None


def _is_new_and_update_seen(tpl, seen, idk=ID_KEY):
    """
    Search a tuple `tpl` from seen and update it as needed.

    >>> seen = set()
    >>> tpl = ('A', (('id', 1), ('a', "AAA")))
    >>> _is_new_and_update_seen(tpl, seen)
    True
    >>> _is_new_and_update_seen(tpl, seen)
    False
    """
    tid = _tuple_id(tpl, idk=idk)  # (relvar, id), id is not None.
    if tid not in seen:
        seen.add(tid)
        return True

    return False


def _gen_relvar_from_dic(dic):
    """
    Generate relvar from given dict `dic` to avoid conflict between relvar and
    key name in the dict.
    """
    keys = sorted(dic.keys())
    return "relvar_%s" % keys[0] if len(keys) == 1 else _gen_relvar(*keys)


def _is_list_item(val):
    """
    :return: True if given `val` is a list of items or False

    >>> all(_is_list_item(x) for x in (['a'], (1, )))
    True
    >>> all(not _is_list_item(x) for x in (None, '', {}, Ref(1, 2)))
    True
    """
    return (val and m9dicts.utils.is_list_like(val) and
            not isinstance(val, Ref))


def _ndict_to_rels_itr(dic, seen, relvar=None, idk=ID_KEY):
    """
    Convert a dict to tuples of (relvar, tuples) where relvar represents the
    name of the relations and tuples are a list of tuples of each tuple
    represents a relation between key and value, and yields tuples one by one.

    .. note:: Yielded relations may be duplicated.

    :param dic: A dict or dict-like object maybe nested
    :param seen: A set object to search items seen previously
    :param relvar: Relation variable
    :param idk: Key to identify the item

    :return: A list of (<relvar>, [tuple of key and value])

    >>> f = lambda *args: sorted(_ndict_to_rels_itr(*args))
    >>> s = set()
    >>> f(dict(A=dict(id=0, a=1), id=1),
    ...   s, "X")  # doctest: +NORMALIZE_WHITESPACE
    [('A', (('a', 1), ('id', 0))),
     ('X', (('A', Ref(relvar='A', id=0)), ('id', 1)))]
    """
    if not dic:
        return  # `dic` is empty.

    if relvar is None:
        relvar = _gen_relvar_from_dic(dic)

    litems = [(k, v) for k, v in dic.items() if _is_list_item(v)]
    rdic = dict(kv for kv in dic.items() if kv not in litems)
    pid = rdic[idk] if idk in rdic else _gen_id(rdic)

    for key, val in litems:
        crelvar = _gen_relvar(relvar, key) if key == relvar else key
        for item in val:  # Yield relations for each items.
            ldic = {key: item, relvar: Ref(relvar, pid)}
            for tpl in _ndict_to_rels_itr(ldic, set(), relvar=crelvar,
                                          idk=idk):
                yield tpl

    for key, val in rdic.items():
        if m9dicts.utils.is_dict_like(val):
            crelvar = _gen_relvar(relvar, key) if key == relvar else key
            if idk in val and not any(isinstance(v, Ref) for v
                                      in val.values()):
                refid = val[idk]
            else:
                refid = _gen_id(val)
                val[idk] = refid

            rdic[key] = Ref(crelvar, refid)  # Replace rdic[key] with Ref.
            for tpl in _ndict_to_rels_itr(val, seen, relvar=crelvar, idk=idk):
                yield tpl

    tpl = _rank_1_dict_to_rels(rdic, relvar=relvar, idk=idk)
    if _is_new_and_update_seen(tpl, seen, idk=idk):
        yield tpl


def dict_to_rels_itr(dic, name=None):
    """
    Convert nested dict[s] to tuples of relation name and relations of items in
    the dict, and yields each pairs.

    :param dic: A dict or dict-like object
    :param name: Name for relations of items in `dic`
    :return: A list of (<relvar>, [tuple of key and value or Ref object])
    """
    seen = set()
    for rls in _ndict_to_rels_itr(dic, seen, relvar=name):
        yield rls


def dict_to_rels(dic, name=None):
    """
    Convert nested dict[s] to tuples of relation name and relations of items in
    the dict, and yields each pairs.

    :param dic: A dict or dict-like object
    :param name: Name for relations of items in `dic`
    :return: A list of (<relvar>, [tuple of key and value or Ref object])
    """
    fst = operator.itemgetter(0)
    rels = dict_to_rels_itr(dic, name=name)
    return [(k, sorted(set(t[1] for t in g)))
            for k, g in itertools.groupby(sorted(rels, key=fst), fst)]

# vim:sw=4:ts=4:et:
