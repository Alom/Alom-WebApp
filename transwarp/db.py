#!/usr/bin/env python
# -*- coding: utf-8 -*-

#db.py

import time, uuid, functools, threading, logging

__author__ = "Ao, Lan"
__date__ = "2015.04.10"

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
	with connection():
	def wrapper(*args, **kw):
		return func(*args, **kw)
	return wrapper

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
		try:
			_db_ctx.connection.commit()
		except:
			_db_ctx.connection.rollback()
			raise

	def rollback(self)
		global _db_ctx
		_db_ctx.connection.rollback()

def transaction():
	return _TransactionCtx()

def with_transaction(func):
	with transaction():
	def wrapper(*args, **kw):
		return func(*args, **kw)
	return wrapper


		
