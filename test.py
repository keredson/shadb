import tempfile
import shadb
import json
import pytest
from dataclasses import dataclass
import collections


def test_unique_index():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_id', lambda o: o.get('id'), unique=True)
    db.store({'resourceType':'X', 'id':'y', 'data':'z'}, commit=True)
    assert db.doc.by_id.get('y')['data'] == 'z'

def test_unique_index_no_doc():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_id', lambda o: o.get('id'), unique=True)
    with pytest.raises(KeyError):
      db.idx.by_id['y']

def test_unique_index_empty_get():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_id', lambda o: o.get('id'), unique=True)
    assert db.idx.by_id.get('y', default=42) == 42

def test_empty_index_get():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_id', lambda o: o.get('id'))
    assert db.idx.by_id.get('y') == []

def test_empty_index():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_id', lambda o: o.get('id'))
    assert list(db.idx.by_id['y']) == []

def test_non_unique_index():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_type', lambda o: o.get('resourceType'))
    db.store({'resourceType':'X', 'id':'y', 'data':'z'}, commit=True)
    assert list(db.docs.by_type.get('X')) == [{'resourceType':'X', 'id':'y', 'data':'z'}]

def test_keys():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_id', lambda o: o.get('id'))
    db.store({'id':'y'}, commit=True)
    db.store({'id':'z'}, commit=True)
    assert list(db.idx.by_id.keys()) == ['y', 'z']
    assert list(db.docs.by_id.keys()) == ['y', 'z']

def test_keys_unique():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_id', lambda o: o.get('id'), unique=True)
    db.store({'id':'y'}, commit=True)
    db.store({'id':'z'}, commit=True)
    assert list(db.idx.by_id.keys()) == ['y', 'z']
    assert list(db.doc.by_id.keys()) == ['y', 'z']

def test_keys_like():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_id', lambda o: o.get('id'))
    db.store({'id':'alice'}, commit=True)
    db.store({'id':'bob'}, commit=True)
    assert list(db.idx.by_id.keys(like='al%')) == ['alice']
    assert list(db.docs.by_id.keys(like='al%')) == ['alice']

def test_items_like():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_id', lambda o: o.get('id'))
    db.store({'id':'alice'}, commit=True)
    db.store({'id':'bob'}, commit=True)
    assert list(db.idx.by_id.items(like='al%')) == [('alice', ['obj/a/l/i/c/obj-alice.json'])]
    assert list(db.docs.by_id.items(like='al%')) == [('alice', [{'id':'alice'}])]

def test_values_like():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_id', lambda o: o.get('id'))
    db.store({'id':'alice'}, commit=True)
    db.store({'id':'bob'}, commit=True)
    assert list(db.idx.by_id.values(like='al%')) == [['obj/a/l/i/c/obj-alice.json']]
    assert list(db.docs.by_id.values(like='al%')) == [[{'id':'alice'}]]

def test_items():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_id', lambda o: o.get('id'))
    db.store({'id':'y'}, commit=True)
    db.store({'id':'z'}, commit=True)
    assert list(db.idx.by_id.items()) == [('y', ['obj/y/obj-y.json']), ('z', ['obj/z/obj-z.json'])]
    assert list(db.docs.by_id.items()) == [('y', [{'id': 'y'}]), ('z', [{'id': 'z'}])]

def test_items_unique():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_id', lambda o: o.get('id'), unique=True)
    db.store({'id':'y'}, commit=True)
    db.store({'id':'z'}, commit=True)
    assert list(db.idx.by_id.items()) == [('y', 'obj/y/obj-y.json'), ('z', 'obj/z/obj-z.json')]
    assert list(db.doc.by_id.items()) == [('y', {'id': 'y'}), ('z', {'id': 'z'})]

def test_values():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_id', lambda o: o.get('id'))
    db.store({'id':'y'}, commit=True)
    db.store({'id':'z'}, commit=True)
    assert list(db.idx.by_id.values()) == [['obj/y/obj-y.json'], ['obj/z/obj-z.json']]
    assert list(db.docs.by_id.values()) == [[{'id': 'y'}], [{'id': 'z'}]]

def test_values_unique():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_id', lambda o: o.get('id'), unique=True)
    db.store({'id':'y'}, commit=True)
    db.store({'id':'z'}, commit=True)
    assert list(db.idx.by_id.values()) == ['obj/y/obj-y.json', 'obj/z/obj-z.json']
    assert list(db.doc.by_id.values()) == [{'id': 'y'}, {'id': 'z'}]

def test_count_by_key():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_type', lambda o: o.get('resourceType'))
    db.store({'resourceType':'X', 'id':'y'}, commit=True)
    db.store({'resourceType':'X', 'id':'z'}, commit=True)
    assert db.idx.by_type.count_by_key() == {'X':2}
    assert db.docs.by_type.count_by_key() == {'X':2}

def test_uncommitted():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_type', lambda o: o.get('resourceType'))
    db.store({'resourceType':'X', 'id':'y', 'data':'z'}, commit=False)
    assert list(db.docs.by_type.get('X')) == [{'resourceType':'X', 'id':'y', 'data':'z'}]

def test_uncommitted_unique_index():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_id', lambda o: o.get('id'), unique=True)
    db.store({'resourceType':'X', 'id':'y', 'data':'z'}, commit=False)
    assert db.doc.by_id.get('y')['data'] == 'z'

def test_delete():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_type', lambda o: o.get('resourceType'))
    fn = db.store({'resourceType':'X', 'id':'y', 'data':'z'}, commit=True)
    assert db.idx.by_type['X'] == [fn]
    db.delete(fn, commit=True)
    assert db.idx.by_type['X'] == []

def FAILING_test_delete_uncommitted():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_type', lambda o: o.get('resourceType'))
    fn = db.store({'resourceType':'X', 'id':'y', 'data':'z'})
    assert db.idx.by_type['X'] == [fn]
    db.delete(fn, commit=True)
    assert db.idx.by_type['X'] == []

def test_multiple_values():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_word', lambda o: o.get('data').split())
    fn = db.store({'data':'derek anderson'}, commit=True)
    assert db.idx.by_word['derek'] == [fn]
    assert db.idx.by_word['anderson'] == [fn]
    assert db.idx.by_word['henderson'] == []

def test_complex_values():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_ngram', lambda o: tuple(o.get('data').split()))
    fn = db.store({'data':'derek anderson'}, commit=True)
    assert db.idx.by_ngram[('derek',)] == []
    assert db.idx.by_ngram[('derek','anderson')] == [fn]

def test_dataclass():
  @dataclass
  class User:
     id: int
     name: str
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, classes=[User])
    fn = db.store(User(1, 'Alice'), commit=True)
    assert fn=='User/1/User-1.json'
    o = db.load(fn)
    assert o==User(id=1, name='Alice')

def test_namedtuple():
  User = collections.namedtuple('User', 'id name')
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, classes=[User])
    fn = db.store(User(1, 'Alice'), commit=True)
    assert fn=='User/1/User-1.json'
    o = db.load(fn)
    assert o==User(id=1, name='Alice')
  

def test_fts():
  with tempfile.TemporaryDirectory() as td:
    db = shadb.SHADB(td, init=True)
    db.add_index('by_text', lambda o: json.dumps(o), fts=True)
    fn = db.store({'data':'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Quisque et luctus arcu, et ornare mi. 2010-10-01'}, commit=True)
    assert db.idx.by_text['ipsum'] == [fn]
    assert db.idx.by_text['lorem'] == [fn]
    assert db.idx.by_text['consectetur elit'] == [fn]
    assert db.idx.by_text['consectetur NOT elit'] == []
    assert db.idx.by_text['consectetur NOT derek'] == [fn]
    assert db.idx.by_text['consectetur OR derek'] == [fn]
    assert db.idx.by_text['consectetur AND derek'] == []
    assert db.idx.by_text['derek'] == []
    assert db.idx.by_text['consect'] == []
    assert db.idx.by_text['consect*'] == [fn]
    assert db.idx.by_text['consectetur OR derek'] == [fn]
    assert db.idx.by_text['consectetur derek OR amet'] == [fn]
    assert db.idx.by_text['consectetur derek'] == []
    assert db.idx.by_text['2010-10-01'] == [fn]
    assert db.idx.by_text['"2010-10-01"'] == [fn]
    assert db.idx.by_text["'2010-10-01'"] == [fn]
    assert db.idx.by_text['2010/10/01'] == [fn]
    assert db.idx.by_text['consectetur not elit'] == []
    assert db.idx.by_text['consectetur not derek'] == [fn]
    assert db.idx.by_text['consectetur or derek'] == [fn]
    assert db.idx.by_text['consectetur and derek'] == []


