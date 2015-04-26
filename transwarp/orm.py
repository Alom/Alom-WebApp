#orm.py

_author_ = "Ao, Lan"
_date_ = "2015.04.14"

class ModelMetaclass(type):
	def __new__(cls, name, bases, attrs):
		if name == "Model":
			return type.__new__(cls, name, bases, attrs)
		mappings = dict()
		for k, v in attrs.iteritems():
			if isinstance(v, Field):
				mappings[k] = v
		for k in mappings.iterkeys():
			attrs.pop(k)
		attrs["__table__"] = name
		attrs["__mappings__"] = mappings
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
	def __init__(self, name, column_type):
		self.name = name
		self.column_type = column_type

	def __str__(self):
		return '<%s,%s>' %(self.__classname__.__name__, self.name)

class IntegerField(Field):
	def __init__(self, name):
		super().__init__(name, 'varchar(100)')

class StringField(Field):
	def __init__(self, name):
		super().__init__(name, 'bigint')





