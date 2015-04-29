#!/usr/bin/env python
# -*- coding: utf-8 -*-

#orm.py

__author__ = "Ao, Lan"
__date__ = "2015.04.14"

import time, logging

import db

class ModelMetaclass(type):
	def __new__(cls, name, bases, attrs):
		if name == "Model":
			return type.__new__(cls, name, bases, attrs)
		
		#why i need to store subclasses????
		if not hasattr(cls, 'subclasses'):
			cls.subclasses = {}

		if not name in cls.subclasses:
			cls.subclasses[name] = name
		else:
			logging.info('Redefine class: %s' %(name))
		
		
		#process mappings for fields
		mappings = dict()
		primary_key = None
		for k, v in attrs.iteritems():
			if isinstance(v, Field):
				if not v.name:
					v.name = name
				
				#handle the primary key cases
				if v.primary_key:
					if primary_key:
						raise TypeError("There is more than 1 primary key in class: %s" %(name))
					if v.nullable:
						logging.warning("The primary key should not be null.")
						v.nullable = False
					if v.updatable:
						logging.warning("The primary key should not be updatable.")
						v.updatabale = False
					primary_key = v
				mappings[k] = v
		if not primary_key:
			raise TypeError("There is no primary key in class: %s" %(name))
		
		for k in mappings:
			attrs.pop(k)

		attrs["__primary_key__"] = primary_key
		if not '__table__' in attrs:
			attrs['__table__'] = name.lower()
		attrs["__mappings__"] = mappings
		attrs['__sql__'] = lambda self: _gen_sql(attrs['__table__'], mappings)
		
		return type.__new__(cls, name, bases, attrs)

class Model(dict):
	__metaclass__ = ModelMetaclass
	
	def __init__(self, **kw):
		super(Model, self).__init__(**kw)

	def __getattr__(self, key):
		try:
			return self[key]
		except:
			raise AttributeError('Dict object has no attribute %s' % key)

	def __setattr__(self, key, value):
		self[key] = value
	
	@classmethod
	def get(cls, pk):
		d = db.select_one('select * from %s where %s=?' %(cls.__table__, class.__primary_key__.name) ,pk)
		return cls(**d) if d else None

	def insert(self):
		params = {}
		for k,v in self.__mappings__.iteritems():
			params[v.name] = getattr(self,k)
		db.insert(self.__table__, **params)
		return self

	def find_first(self):
		
		pass

	
	def find_all(self):
		pass

	def find_by(self,**kw):
		pass

	def count_all(self):
		pass

	def count_by(self, **kw):
		pass

	def update(self):
		pass

	def delete(self):
		pass


class Field(object):
	_count = 0
	
	def __init__(self, **kw):
		self.name = kw.get('name',None)
		self._default = kw.get('default',None)
		self.primary_key = kw.get('primary_key', False)
		self.nullable = kw.get('nullable', False)
		self.updatable = kw.get('updatable', True)
		self.insertable = kw.get('insertable', True)
		slef.ddl = kw.get('ddl','')
		self._order = Field._count
		Field._count = Field._count + 1
		
	@property
	def default(self):
		d = self._default
		return d() if callable(d) else d
		
	def __str__(self):
		s = ['<%s:%s,%s,default(%s),'%(self.__class__.__name__, self.name, self.ddl, self._default)]
		self.nullable and s.append('N')
		self.updatable and s.appned('U')
		self.insertable and s.append('I')
		s.append('>')
		return ''.join(s)
		
	

	def __str__(self):
		return '<%s,%s>' %(self.__classname__.__name__, self.name)

class IntegerField(Field):
	def __init__(self, **kw):
		if not 'default' in kw:
			kw['default'] = 0
		if not 'ddl' in kw:
			kw['ddl'] = 'bigint'
		super(IntegerField, self).__init__(**kw)

class StringField(Field):
	def __init__(self, **kw):
		if not 'default' in kw:
			kw['default'] = ''
		if not 'ddl' in kw:
			kw['ddl'] = 'varchar(255)'
		super(StringField, self).__init__(**kw)
		
class FloatField(Field):
	def __init__(self, **kw):
		if not 'default' in kw:
			kw['default'] = 0.0
		if not 'ddl' in kw:
			kw['ddl'] = 'real'
		super(FloatField, self).__init__(**kw)

class BooleanField(Field):
	def __init__(self, **kw):
		if not 'default' in kw:
			kw['default'] = False
		if not 'ddl' in kw:
			kw['ddl'] = 'bool'
		super(BooleanField, self).__init__(**kw)

class TextField(Field):
	def __init__(self, **kw):
		if not 'default' in kw:
			kw['default'] = ''
		if not 'ddl' in kw:
			kw['ddl'] = 'text'
		super(TextField, self).__init__(**kw)

class BlobField(Field):
	def __init__(self, **kw):
		if not 'default' in kw:
			kw['default'] = ''
		if not 'ddl' in kw:
			kw['ddl'] = 'blob'
		super(BlobField, self).__init__(**kw)

class VersionField(Field):
	def __init__(self, name):
		super(VersionField, self).__init__(name=name, default=0, ddl='bigint')


			





