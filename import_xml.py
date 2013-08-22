#!/usr/bin/env python

# Author: 		Toma Kraft
# Date: 		Aug 21st, 2013

from mongo import Mongo
import subprocess
import sys
import json

class Import_XML(Mongo):
	def __init__(self, host=None, port=None, db=None, default_collection='default'):
		# dict name, persistant, host, port, db, verbose
		Mongo.__init__(self, host, port, db, default_collection)
		self.setMtag('[Import_XML]')

	def xml2json(self, xml_filename, root=None, split=None):
		
		xml_filename = xml_filename.split('.')[0]
		
		xml2json_command = [
							'python',
							'xml2json.py',
							'-t',
							'xml2json',
							'-o',
							xml_filename+'.json',
							xml_filename+'.xml'
		]
		
		print self.mTag, 'about to call xml2json_command on file:', xml_filename
		subprocess.call(xml2json_command)
		print self.mTag, 'converted', xml_filename, 'to a json format'
		
		json_data = open(xml_filename+'.json','rb')
		data = json.load(json_data)
		
		if root:
			if not root in data:
				return
			
			if split:	
				if not split in data[root]:
					return
					
				
				for item in data[root][split]:
					self.add(item)
		else:
			if split:	
				if not split in data[root]:
					return
				for item in data[split]:
					self.add(item)
def main(args):
	xml = Import_XML(args['-h'],args['-p'],args['-d'], args['-c'])
	
	if '-f' in args:
		root = None
		split = None
		if '-s' in args:
			split = args['-s']
		
		if '-r' in args:
			root = args['-r']
			
		xml.xml2json(args['-f'], root, split)
	
	

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


