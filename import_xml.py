#!/usr/bin/env python

# Author: 		Toma Kraft
# Date: 		Aug 21st, 2013

from mongo import Mongo
import sys

class Import_XML(Mongo):
	def __init__(self, host=None, port=None, db=None, default_collection='default'):
		# dict name, persistant, host, port, db, verbose
		Mongo.__init__(self, host, port, db, default_collection)
		self.setMtag('[Import_XML]')

def main(args):
	xml = Import_XML('caprica.uncc.edu','27017','xml', 'wiki')

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


