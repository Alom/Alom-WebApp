#!/usr/bin/env python
# -*- coding: utf-8 -*-

#db.py
__author__ = "Ao, Lan"
__date__ = "2015.04.10"

'''
Database operation module
'''

import time, uuid, functools, threading, logging

class Dict(dict):
	def __init__(self, name=(), values=(), **kw):
		super(Dict, self).__init__(**kw)
		for k, v in name, values:
			self[k] = v
			
	def __getattr__(self, key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Dict' object has no attribute %s" %key)
			
	def __setattr__(self, key, value):
		self[key] = value
		
def next_id(t=None):
	if t is None:
		t = time.time()
	return '%015d%s000' %(int(t*1000),uuid.uuid4().hex)
	
def _profiling(start, sql=''):
	t = time.time() - start
	if t > 0.1:
		logging.warning('[PROFILING] [DB] %s: %s' %(t, sql))
	else:
		logging.info('[PROFILING] [DB] %s: %s' %(t, sql))
		
class DBError(Exception):
	pass

class MultiColumnsError(DBError):
	pass

class _LasyConnection(object):
	
	def __init__(self):
		self.connection = None
		
	def cursor(self):
		if self.connection is None:
			connection = engine.connect()
			logging.info('Open connection <%s>...' %hex(id(connection)))
			self.connection = connection
		return self.connection.cursor()
		
	def commit(self):
		self.connection.commit()
		
	def rollback(self):
		self.connection.rollback()
		
	def cleanup(self):
		if self.connection:
			connection = self.connection
			self.connection = None
			logging.info
			connection.close()
	

		
		

class _Engine(object):
	def __init__(self, connect):
		self._connect = connect
	def connect(self):
		return self._connect()

engine = None

class _DbCtx(threading.local):
	def __init__(self):
		self.connection = None
		self.transactions = 0

	def is_init(self):
		return not self.connection is None

	def init(self):
		self.connection = _LasyConnection()
		self.transactions = 0

	def cleanup(self):
		self.connection.cleanup()
		self.connection = None

	def cursor(self):
		return self.connection.cursor()

_db_ctx = _DbCtx()

def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
	import mysql.connector
	global engine
	if engine is not None:
		raise DBError('Engine is already initialized.')
	params = dict(user=user, password=password, database=database, port=port)
	defaults = dict(use_unicode=True, charset="utf8", collation='utf8_general_ci',autocommit=False)
	for k,v in defaults.iteritems():
		params[k] = kw.pop(k,v)
	params.update(kw)
	params['buffered'] = True
	engine = _Engine(lambda: mysql.connector.connect(**params))
	#test connection
	logging.info('Init mysql engine <%s> ok.' %hex(id(engine)))
	


class _ConnectionCtx(object):
	def __enter__(self):
		global _db_ctx
		self.should_cleanup = False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_cleanup = True
		return self

	def __exit__(self, exctype, excvalue, traceback):
		global _db_ctx
		if self.should_cleanup:
			_db_ctx.cleanup()

def connection():
	return _ConnectionCtx()

def with_connection(func):
	@functools.wraps(func)
	def wrapper(*args, **kw):
		with _ConnectionCtx():
			return func(*args, **kw)
	return wrapper

class _TransactionCtx(object):
	def __enter__(self):
		global _db_ctx
		self.should_close_conn = False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_close_conn = True
		_db_ctx.transactions = _db_ctx.transactions + 1
		return self

	def __exit__(self,exctype, excvalue, traceback):
		global _db_ctx
		_db_ctx.transactions = _db_ctx.transactions - 1
		try:
			if _db_ctx.transactions == 0:
				if exctype == None:
					self.commit()
				else:
					self.rollback()
		finally:
			if self.should_close_conn:
				_db_ctx.cleanup()
	
	def commit(self):
		global _db_ctx
		logging.info('commit transaction...')
		try:
			_db_ctx.connection.commit()
			logging.info('commit ok.')
		except:
			logging.warning('commit failed. try rollback...')
			_db_ctx.connection.rollback()
			logging.warning('rollback ok.')
			raise

	def rollback(self)
		global _db_ctx
		logging.warning('rollback transaction...')
		_db_ctx.connection.rollback()
		logging.info('rollback ok.')

def transaction():
	'''
	Create a transaction object so can use with statement:
	
	with transaction():
        pass
	
	>>> def update_profile(id, name, rollback):
	...     u = dict(id=id, name=name, email='%s@test.org' % name, passwd=name, last_modified=time.time())
	...     insert('user', **u)
	...     r = update('update user set passwd=? where id=?', name.upper(), id)
	...     if rollback:
	...         raise StandardError('will cause rollback...')
	>>> with transaction():
	...     update_profile(900301, 'Python', False)
	>>> select_one('select * from user where id=?', 900301).name
	u'Python'
	>>> with transaction():
	...     update_profile(900302, 'Ruby', True)
	Traceback (most recent call last):
	   ...
	StandardError: will cause rollback...
	>>> select('select * from user where id=?', 900302)
	[]
	'''
	return _TransactionCtx()

def with_transaction(func):
	@functools.wrap(func)
	def wrapper(*args, **kw):
		_start = time.time()
		with _TransactionCtx():
			return func(*args, **kw)
		_profiling(_start)
	return wrapper
'''
@with_connection
def update(SQL, *args):
	global _db_ctx
	sql_ctx = SQL.replace("?","%s")
	parameter = []
	result_count
	for s in args:
		parameter.append(s)
	cursor = _db_ctx.connection.cursor()
	try:
		cursor.execute(sql_ctx, parameter)
		result_count = cursor.count
		cursor.commit()
	finally:
		cursor.close()
	return result_count
	

@with_connection
def select(SQL, *args):
	global _db_ctx
	#sql_ctx = SQL.replace("?","%s")
	result_set = []
	parameter = []
	for s in args:
		parameter.append(s)
	cursor = _db_ctx.connection.cursor()
	try:
		cursor.execute(sql_ctx, parameter)
		result = cursor.fetchall()
		for item in result:
			length = len(parameter) if len(parameter) <= len(item) else len(item)
			result_dict = {}
			for i in range(length):
				result_dict[parameter[i]] = item[i]
			result_set.append(result_dict)
	finally:
		cursor.close()
	return result_set
'''

def _select(sql, first, *args):
	global _db_ctx
	cursor = None
	sql = sql.replace('?','%s')
	logging.info('SQL: %s, ARGS: %s'%(sql,args))
	try:
		cursor = _db_ctx.connection.cursor()
		cursor.execute(sql, args)
		if cursor.description:
			names = [x[0] for x in cursor.description]
		if first:
			values = cursor.fetchone()
			if not value:
				return None
			return Dict(names, values)
		return [Dict(names, x) for x in cursor,fetchall()]
	finally:
		if cursor:
			cursor.close()

@with_connection
def select_one(sql, *args):
	return _select(sql, True, *args)
	
@with_connection
def select_int(sql, *args):
	d = _select(sql, True, *args)
	if len(d) != 1:
		raise MultiColumnsError('Expect only one column.')
	return d.values()[0]
	
@with_connection
def select(sql, *args):
	return _select(sql, False, *args)
	
@with_connection
def _update(sql, *args):
	global _db_ctx
	cursor = None
	sql = sql.replace('?', '%s')
	logging.info('SQL: %s, ARGS: %s' %(sql, args))
	try:
		cursor = db_ctx.connection.cursor()
		cursor.execute(sql, args)
		r = cursor.rowcount
		if _db_ctx.transactions = 0:
			logging.info('auto commit')
			_db_ctx.connection.commit()
		return r
	finally:
		if cursor:
			cursor.close()
			
def insert(table, **kw):
	cols, args = zip(*kw.iteritems())
	sql = 'insert into `%s` (%s) values (%s)' % (table, ','.join(['`%s`' % col for col in cols]), ','(['?' for i in range(len(cols))]))
	return _update(sql, *args)

def update(sql, *args):
	return _update(sql, *args)
	
if __name__=='__main__':
	logging.basicConfig(level=logging.DEBUG)
	create_engine('www-data', 'www-data', 'test')
	update('drop table if exist user')
	update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
	import doctest
	doctest.testmod()
