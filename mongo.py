#!/usr/bin/env python

# Author: 		Toma Kraft
# Date: 		July 9th, 2013

import logging
import pymongo 
import sys
import traceback #traceback.print_exc(file=sys.stdout)
import nltk
import csv
import types
import math

import datetime
from dateutil.relativedelta import relativedelta

def removeNonAscii(s): return "".join(i for i in s if ord(i)<128)

class MongoBase(object):
	def __init__(self, host=None, port=None, db_name=None, verbose=True):
		#logging.basicConfig(filename='logs/mongo.log',level=logging.DEBUG)

		self.mTag = '[MongoBase]'
		self.verbose = verbose


		if not host:
			host = 'localhost'

		if not port:
			port = '27017'
		
		if not db_name:
			db_name = 'default'
		
		# set class vars
		self.host = host
		self.port = port
		self.db_name = db_name

		mongodb_uri = 'mongodb://'+host+':'+port

		try:
			# pymongo objects
			self.conn = pymongo.Connection(mongodb_uri)
			self.db = self.conn[self.db_name]

			if verbose:
				print self.mTag,'successfully connected to:', mongodb_uri, 'using db:', db_name
			logging.info('[CREATED] '+self.__str__())
		except:
			print self.mTag,'[CONNECTION ERROR] [__init__]'
			traceback.print_exc(file=sys.stdout)
			self.conn = None


	def close(self):
		if self.conn:
			self.conn.disconnect()

			if self.verbose:
				print self.mTag, 'Closed connection to', self.host,'db:',self.db_name

	def setMtag(self, mTag):
		self.mTag = mTag

	def __str__(self):
		host = self.host
		port = self.port
		db_name = self.db_name
		mTag = self.mTag

		return mTag+' object: '+'mongodb://'+host+':'+port+'  db: '+db_name

	def __repr__(self):
		return self.__str__()

	def __exit__(self):
		self.close()
	
	def __del__(self):
		self.__exit__()
		
class MongoDict(MongoBase):
	def __init__(self, dictName, persistant=True, host=None, port=None, db='dict', verbose=False):
		# All MongoDicts stored in db='dict' unless specified otherwise
		MongoBase.__init__(self, host, port, db, verbose)
		self.setMtag('[MongoDict]')

		self.persistant = persistant
		self.dictName = dictName
		#self.cache = {}
		#self.cache[dictName] = {}

	def __call__(self, newName):
		self.dictName = newName

		#if not newName in self.cache:
		#	self.cache[newName] = {}

	def __setitem__(self, key, value):
		name = self.dictName
		mTag = self.mTag
		db = self.db
		conn = self.conn
		verbose = self.verbose
		#cache = self.cache

		if conn:
			value['_id'] = key
			db[name].save(value)

			# save for future reference
			#cache[name][key] = value

			if verbose:
				print mTag, '[__setitem__] id:', key, 'doc:', value

		else:
			print mTag, '[CONNECTION ERROR] [__getitem__]'

	def __getitem__(self, key):
		name = self.dictName
		mTag = self.mTag
		db = self.db
		conn = self.conn
		verbose = self.verbose
		#cache = self.cache

		if conn:
			# lookup the cache first
			#if name in cache:
				#if key in cache[name]:
					#return cache[name][key]
			#else:
				#cache[name] = {}


			# key not in cache, load from db if availble
			result = list(db[name].find({'_id':key}).limit(1))

			if result:
				if verbose:
					print mTag, '[__getitem__] _id:',key, 'doc:', result[0]

				# so that furture request for key will be faster
				#cache[name][key] = result[0]

				return result[0]
			else:
				if verbose:
					print mTag, '[__getitem__] _id:', key, 'not found'
				return False
		else:
			print mTag, '[CONNECTION ERROR] [__getitem__]'

	def __iter__(self):
		name = self.dictName
		mTag = self.mTag
		db = self.db
		conn = self.conn
		verbose = self.verbose

		if conn:
			results = list(db[name].find({},{'_id':1}))

			if results:
				for doc in results:
					yield doc['_id']
			else:
				yield 

	def __len__(self):
		name = self.dictName
		mTag = self.mTag
		db = self.db
		conn = self.conn
		verbose = self.verbose

		if conn:
			return db[name].count()
		else:
			return 0

	def __contains__(self, item):
		name = self.dictName
		mTag = self.mTag
		db = self.db
		conn = self.conn
		verbose = self.verbose
		#cache = self.cache

		if conn:
			#if name in cache:
				#if item in cache[name]:
					#return True
			#else:
				#cache[name] = {}

			result = list(db[name].find({'_id':item}).limit(1))

			if result:
				# already made a request to db, might aswell save the result
				#cache[name][item] = result[0]
				return True
			else: 
				return False
		else:
			return False


	def __exit__(self):
		name = self.dictName
		mTag = self.mTag
		db = self.db
		conn = self.conn
		verbose = self.verbose
		persistant = self.persistant # if data is persistant leave intact, otherwise drop collection after deletion

		if conn:
			if not persistant:
				db[name].drop()

				if verbose:
					print mTag, '[__exit__] dropped collection:', name

		self.close()
		
		
class Mongo(MongoDict):
	def __init__(self, host=None, port=None, db=None, default_collection='default'):
		# dict name, persistant, host, port, db, verbose
		MongoDict.__init__(self, default_collection, True, host, port, db, True)
		self.setMtag('[Mongo]')


	def __getattr__(self, name):
		mTag = self.mTag
		db = self.db
		conn = self.conn
		verbose = self.verbose

		if not conn:
			logging.debug(self.__str__()+' -- no conn!')

		# sets the collection name
		self(name)

		def handle_doc(_id=None, doc=None, update=None, bulk=None, agg=None):
			# add a document to mongo
			if doc:
				if not _id:
					_id = doc['_id']

				self.__setitem__(_id, doc)
				return

			# retreives a document from mongo
			if _id:
				return self.__getitem__(_id)

			# updates the _id given the update mongo query
			if update:
				if '_id' in update:
					_id = update['_id']

				del update['_id']
				query = {
					'_id':_id
				}
				self.db[name].update(query, update, upsert=True) # True for upsert
				return
				# need to increment the fields passed in

			# adds a list of documents
			if bulk:
				self.db[name].insert(bulk)
				return
			
			# should be a list of dictionaries specifing the aggregation pipeline
			if agg:
				return self.db[name].aggregate(agg)

		return handle_doc
		
		
def main(args):
	m = Mongo('caprica.uncc.edu','27017','xml', 'wiki')

if __name__ == '__main__':
	argv = sys.argv[1:]
	args = {}
	if argv:
		args = {}

		# there should be an specifier for each parameter
		# so there should always be an even number of args
		# set the first item to be the specifier of the dict
		# and the second is the value 
		if len(argv) % 2 == 0:
			for i in range(len(argv)-1):
				args[argv[i]] = argv[i+1]

	main(args)
