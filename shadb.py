import pathlib
import sqlite3
import os
import sh
import urllib.parse
import json
import re
import tempfile
import collections
import dataclasses
import uuid
import inspect, hashlib


class SHADB:

  def __init__(self, git_path, init=True, id_key='id', type_key='type', classes=[]):
    self._type_key = type_key
    self._id_key = id_key
    self._git_path = pathlib.Path(git_path).absolute()
    self._sqlite_path = self._git_path / 'idx.db'
    self._git = sh.git.bake(C=self._git_path)
    self._classes = {cls.__name__:cls for cls in classes}
    self._current_commit = None
    try:
      self._git.status()
    except sh.ErrorReturnCode_128 as e:
      if init:
        self._git.init()
        with open(self._git_path/'.gitignore', 'w') as f:
          f.write('idx.db\n')
        self._git.add('.gitignore')
        self._git.commit('.gitignore', m='added .gitignore')
        
      else: raise e
    self.idx = Indices()
    self.doc = UniqueDocIndices()
    self.docs = DocIndices()
    self._init_db()
  
  def _init_db(self):
    with self._connect() as conn:
      cur = conn.cursor()
      cur.execute("CREATE TABLE IF NOT EXISTS indexed_state (name TEXT NOT NULL PRIMARY KEY, last_hash TEXT NOT NULL);")
  
  def _update_git_path(self, git_path, init=True):
    self.__init__(git_path, init=init)
    for idx in self.idx.__dict__.values():
      idx._init_db()

  def add_index(self, name, f, **kwargs):
    if hasattr(self.idx, name): raise Exception('conflicting name')
    index = Index(self, name, f, **kwargs)
    index.update()
    setattr(self.idx, name, index)
    if index._unique:
      setattr(self.doc, name, DocIndex(index))
    else:
      setattr(self.docs, name, DocIndex(index))

  def _connect(self):
    conn = sqlite3.connect(self._sqlite_path)
    #conn.set_trace_callback(print)
    return conn

  def status(self):
    changes = self._git.status('--porcelain')
    changes = changes.strip()
    changes = changes.splitlines()
    changes = [s.split() for s in changes if s]
    return changes
  
  def log(self, *fns):
    changes = []
    change = None
    with tempfile.NamedTemporaryFile() as tf:
      print('tf.name', tf.name)
      self._git.log(*fns, output=tf.name)
      with open(tf.name,'r') as f:
        for line in f.readlines():
          if line.startswith(' '): continue
          line = line.strip()
          if line.startswith('commit'):
            change = {'commit':line.split()[1]}
            changes.append(change)
          if line.startswith('Author:'):
            author = line.split()[1:]
            change['author'] = {
              'name': ' '.join(author[:-1]),
              'email': author[-1],
            }
          if line.startswith('Date:'):
            _, date = line.split(maxsplit=1)
            change['date'] = date
    print('changes', changes)
    return changes

  def store(self, *objects):
    if self._current_commit:
      return self._current_commit.store(*objects)
    else:
      # don't commit if we're not in a commit
      return self.commit().store(*objects)
      

  def dump(self, o, fn, commit=False, _update_idx=True):
    exists = os.path.isfile(os.path.join(self._git_path, fn))
    with open(os.path.join(self._git_path, fn),'w') as f:
      json.dump(o, f, indent=2, sort_keys=True)
    #print('dumping', fn, o.json())
    if not exists:
      self._git.add(fn)
    if commit:
      self.commit(fn)
    elif _update_idx:
      self.idx.update(also_fns=[fn])

  def load(self, fn, ignore_fnf=False):
    try:
      with open(os.path.join(self._git_path, fn),'r') as f:
        o = json.load(f)
        if '__dataclass__' in o:
          cls = self._classes.get(o['__dataclass__'])
          del o['__dataclass__']
          o = cls(**o)
        elif '__namedtuple__' in o:
          cls = self._classes.get(o['__namedtuple__'])
          del o['__namedtuple__']
          o = cls(**o)
        return o
    except FileNotFoundError as e:
      if ignore_fnf: return None
      else: raise e

  def delete(self, *fns, commit=False):
    self._git.rm('-f', *fns)
    if commit:
      self.commit(*fns)
    else:
      self.idx.update(also_fns=fns)
  
  def __contains__(self, fn):
    return os.path.isfile(os.path.join(self._git_path, fn))

  def commit(self, *fns_or_objects, update=True):
    if not fns_or_objects: return
    fns = [s.__fhir_fn__ if hasattr(s,'__fhir_fn__') else s for s in fns_or_objects]
    self._git.commit(*fns, m='fhirdb')
    if update:
      self.idx.update()
  
  def __enter__(self):
    self._current_commit = self.commit()
    return self

  def __exit__(self, exc_type, exc_value, exc_traceback):
    self._current_commit.__exit__(exc_type, exc_value, exc_traceback)
    self._current_commit = None
    
  def commit(self, m=None):
    return Commit(self, m=m)
  

class Commit:
  def __init__(self, db, m=None):
    self._db = db
    self._m = m
    self._fns = []

  def __enter__(self):
    return self
    
  def __exit__(self, exc_type, exc_value, exc_traceback):
    if exc_type: self._abort()
    else: self._commit()
  
  def _abort(self):
    if self._fns:
      to_delete = [x[1] for x in self._db.status() if x[0]=='A']
      self._db._git.reset(*self._fns)
      for fn in to_delete:
        os.remove(os.path.join(self._db._git_path, fn))
      self._db.idx.update(also_fns=self._fns)
  
  def _commit(self):
    if self._fns:
      self._db._git.commit(*self._fns, m=self._m)
      #self._db.idx.update()

  def store(self, *objects):
    fns = []
    auto_idxs = [idx for idx in self._db.idx.__dict__.values() if isinstance(idx,Index) and idx._auto]
    unique_idxs = [idx for idx in self._db.idx.__dict__.values() if isinstance(idx,Index) and idx._unique]
    for o in objects:

      # handle known object types      
      if dataclasses.is_dataclass(o):
        cls = o.__class__.__name__
        o = dataclasses.asdict(o)
        o['__dataclass__'] = cls
      elif isinstance(o, tuple) and hasattr(o, '_asdict'):
        cls = o.__class__.__name__
        o = o._asdict()
        o['__namedtuple__'] = cls
      else:
        cls = o.get(self._db._type_key, 'obj')
      
      for idx in auto_idxs:
        idx._autogen(o)
        
      sig = None
      unique_idx = None
      for unique_idx in unique_idxs:
        sig = unique_idx._f(o)
        if sig: break
      sig = sig or str(uuid.uuid4()).replace('-','')
      sig = urllib.parse.quote(str(sig))
      first_4 = sig[:4]
      #if len(first_4) < 4:
      #  first_4 += hashlib.md5(first_4.encode()).hexdigest()
      #  first_4 = first_4[:4]
      cls = urllib.parse.quote(str(cls))

      dn = os.path.join(cls, *first_4)
      if not os.path.isdir(os.path.join(self._db._git_path, dn)):
        os.makedirs(os.path.join(self._db._git_path, dn), exist_ok=True)
      fn = os.path.join(dn, f'{cls}{"-"+unique_idx._name if unique_idx else ""}-{sig}.json')
      self._db.dump(o, fn, _update_idx=False)
      fns.append(fn)
    self._fns.extend(fns)
    self._db.idx.update(also_fns=fns)
    return fns[0] if len(objects)==1 else fns
    

class Index:

  def __init__(self, db, name, attr_or_f, *, unique=False, index=True, index_null=False, fts=False, auto=None):
    if name.startswith('_'): raise ValueError('illegal name - starts with _')
    if not name.isidentifier(): raise ValueError('illegal name - not a python identifier')
    if unique and fts: raise ValueError('you cannot set unique=True and fts=True')
    if auto==True: auto = uuid.uuid4
    if isinstance(attr_or_f, str):
      self._attr = attr_or_f
      self._f = lambda o: getattr(o, attr_or_f) if hasattr(o, attr_or_f) else o.get(attr_or_f)
      version = hashlib.md5(attr_or_f.encode()).hexdigest()
    else:
      if auto: raise ValueError('to use auto, attr_or_f must be a str')
      self._attr = None
      self._f = attr_or_f
      version = hashlib.md5(inspect.getsource(self._f).encode()).hexdigest()
    self._tbl_name = 'idx_%s__V%s' % (name, version)
    self._name = name
    self._db = db
    self._unique = unique
    self._index_null = index_null
    self._index = index
    self._fts = fts
    self._auto = auto
    self._init_db()
  
  def _init_db(self):
    with self._connect() as conn:
      cur = conn.cursor()
      cur.execute('BEGIN;')
      if self._fts:
        cur.execute(f'''
          CREATE VIRTUAL TABLE IF NOT EXISTS "{self._tbl_name}" USING fts5(fn, key);
        ''')
      else:
        cur.execute(f'''
          CREATE TABLE IF NOT EXISTS "{self._tbl_name}"(
            key TEXT {"" if self._index_null else "NOT NULL"} {"PRIMARY KEY" if self._unique else ""},
            fn TEXT NOT NULL
          );
        ''')
      if self._index and not self._fts:
        if not self._unique:
          cur.execute(f'CREATE INDEX IF NOT EXISTS "{self._tbl_name}_idx" on "{self._tbl_name}" (key);')
        cur.execute(f'CREATE INDEX IF NOT EXISTS "{self._tbl_name}_fn_idx" on "{self._tbl_name}" (fn);')
      conn.commit()
  
  def _autogen(self, o):
    id = o.get(self._attr)
    if not id:
      o[self._attr] = id = self._auto()
    return id
      
  def _connect(self):
    return self._db._connect()
  
  def update(self, also_fns=[]):
    with self._connect() as conn:
      #conn.set_trace_callback(print)
      cur = conn.cursor()
      cur.execute('BEGIN')
      results = cur.execute('select last_hash from indexed_state where name=?', (self._tbl_name,)).fetchone()
      last_hash = results[0] if results else getattr(self._db._git, 'hash-object')('/dev/null', t='tree').strip()
      current_hash = getattr(self._db._git, 'rev-parse')('HEAD').strip()
      # git diff ignores --no-color so use ansi2txt
      with tempfile.NamedTemporaryFile() as tf:
        self._db._git.diff('--name-status', last_hash, 'HEAD', output=tf.name)
        with open(tf.name,'r') as f:
          changes = f.read().strip()
      changes = changes.splitlines()
      changes = [s.split() for s in changes if s]
      for fn in also_fns:
        changes.append(('M' if fn in self._db else 'D',fn))
      for status, fn, *other in changes:
        #print('status, fn', status, fn, other)
        if not fn.endswith('.json'): continue
        # https://git-scm.com/docs/git-diff#:~:text=Possible%20status%20letters%20are%3A
        if status=='R100':
          cur.execute(f'update "{self._tbl_name}" set fn=? where fn=?', (other[0], fn))
        if status in 'D' or (status=='M' and not self._unique) or (status.startswith('R') and status!='R100'):
          cur.execute(f'delete from "{self._tbl_name}" where fn=?', (fn,))
        if status in 'ACM' or (status.startswith('R') and status!='R100'):
          if status.startswith('R'):
            fn = other[0]
          try:
            o = self._db.load(fn)
          except FileNotFoundError as e:
            print('FileNotFound:', fn)
            continue
          value = self._f(o)
          if value is not None or self._index_null:
            values = value if isinstance(value, list) else [value]
            for value in values:
              normalized_value = self._normalize(value)
              try:
                cur.execute(f'{"replace" if self._unique else "insert"} into "{self._tbl_name}" (fn,key) values (?,?)', (fn, normalized_value))
              except sqlite3.InterfaceError as e:
                print('failed to set', value, 'for', fn)
                raise e
      cur.execute('replace into indexed_state values (?,?)', (self._tbl_name,current_hash))
      conn.commit()
  
  def all(self):
    with self._connect() as conn:
      cur = conn.cursor()
      q = cur.execute(f'select fn from "{self._tbl_name}"')
      results = q.fetchall()
      return (r[0] for r in results)
  
  def _normalize(self, value):
    if not isinstance(value,str):
      value = json.dumps(value, sort_keys=True)
    return value
      
  def __getitem__(self, key):
    if self._fts:
      cmd_words = set('and or not'.split())
      split_respect_quotes = re.findall(r'(?:[^\s,"]|"(?:\\.|[^"])*")+', key)
      split_respect_quotes = [s.upper() if s.lower() in cmd_words else s for s in split_respect_quotes]
      split_respect_quotes = ['"%s"'%s.strip('"') if re.search('[-/]',s) else s for s in split_respect_quotes]
      key = ' '.join(split_respect_quotes)
    with self._connect() as conn:
      cur = conn.cursor()
      limit = ' limit 1' if self._unique else ''
      if key is None:
        q = cur.execute(f'select fn from "{self._tbl_name}" where value is null'+limit)
      else:
        normalized_key = self._normalize(key)
        cmp_o = 'like' if '%' in normalized_key else '='
        if self._fts: cmp_o = 'MATCH'
        q = cur.execute(f'select fn from "{self._tbl_name}" where key {cmp_o} ?'+limit, (normalized_key,))
      if self._unique:
        result = [r[0] for r in q.fetchall()]
        if result: return result[0]
        else: raise KeyError(key)
      else:
        results = q.fetchall()
        return ([r[0] for r in results])

  def get(self, key, default=None):
    try:
      return self[key]
    except KeyError:
      return default

  def keys(self, like=None):
    with self._connect() as conn:
      cur = conn.cursor()
      if like:
        q = cur.execute(f'select distinct key from "{self._tbl_name}" where key like ?', (like,))
      else:
        q = cur.execute(f'select distinct key from "{self._tbl_name}"')
      results = q.fetchall()
      return (r[0] for r in results)
  
  def items(self, like=None):
    with self._connect() as conn:
      cur = conn.cursor()
      if like:
        q = cur.execute(f'select distinct key, fn from "{self._tbl_name}" where key like ? order by key', (like,))
      else:
        q = cur.execute(f'select distinct key, fn from "{self._tbl_name}" order by key')
      last_key = None
      values = []
      for key, fn in q.fetchall():
        if self._unique: yield key, fn
        else:
          if key != last_key:
            if last_key is not None: yield last_key, values
            last_key = key
            values = []
          values.append(fn)
      if not self._unique: yield key, values
  
  def values(self, like=None):
    return (v for k,v in self.items(like=like))
  
  def count_by_key(self, like=None):
    with self._connect() as conn:
      cur = conn.cursor()
      if like:
        q = cur.execute(f'select key, count(distinct fn) from "{self._tbl_name}" where key like ? group by key', (like,))
      else:
        q = cur.execute(f'select key, count(distinct fn) from "{self._tbl_name}" group by key')
      results = q.fetchall()
      return {r[0]:r[1] for r in results}
  
  def __contains__(self, key):
    return bool(self.get(key))
     
          

class Indices:

  def update(self, **kwargs):
    for idx in self.__dict__.values():
      idx.update(**kwargs)


class DocIndices:
  pass

class UniqueDocIndices:
  pass

class DocIndex:
  def __init__(self, idx):
    self._idx = idx

  def __getitem__(self, key):
    if self._idx._unique:
      return self._idx._db.load(self._idx[key])
    else:
      return (self._idx._db.load(fn) for fn in self._idx[key])

  def get(self, key, default=None):
    try:
      return self[key]
    except KeyError:
      return default
  
  def items(self, like=None):
    if self._idx._unique:
      return ((k,self._idx._db.load(fn)) for k,fn in self._idx.items(like=like))
    else:
      return ((k,[self._idx._db.load(fn) for fn in fns]) for k,fns in self._idx.items(like=like))

  def values(self, like=None):
    return (v for k,v in self.items(like=like))

  def __getattr__(self, attr):
    return getattr(self._idx, attr)
  
    

