#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pymongo import MongoClient, ASCENDING, DESCENDING
from msmjson import debug_message, print_msg
import logging


class MSMMongo:
	def __init__(self, host='localhost', port=27017):
		self.host = host
		self.port = port
		self.client = MongoClient(host, port)

	def add_new_db(self, dbname, data, drop=True, logger=None):
		if logger is not None: logger.info('ADD DB START')
		if drop:
			if logger is not None: logger.info('ADD DB: dropping database "%s" ...' % dbname)
			self.client.drop_database(dbname)
			if logger is not None: logger.info('ADD DB: dropping database finished')
		db = self.client[dbname]
		for gl in data:
			glob = db[gl]	
			if logger is not None: logger.info('ADD DB: Processing %s.%s global' % (dbname, gl))	
			if gl != 'info':			
				records = []
				for i in data[gl]:
					rec = {}
					rec["msm_id"] = i
					for prop in data[gl][i]:
						rec[prop] = data[gl][i][prop]
					records.append(rec)
				if records != []:
					glob.insert(records)
				if logger is not None: logger.info('ADD DB: %s.%s global: %d records processed' % (dbname, gl, len(records)))
			else:
				data[gl]['updates'] = []
				data[gl]['updates'].append(data[gl]['time'])
				glob.insert(data[gl])
		if logger is not None: logger.info('ADD DB: Creating indexes...')
		for gl in data:
			if gl != 'info':
				if logger is not None: logger.info('ADD DB: Creating index for %s.%s collection...' % (dbname, gl))
				db[gl].create_index([("msm_id", ASCENDING)])
		if logger is not None: logger.info('ADD DB END')

	def update_db(self, dbname, data, logger=None):
		if logger is not None: logger.info('UPD DB START')
		db = self.client[dbname]
		for gl in data:
			glob = db[gl]
			if logger is not None: logger.info('UPD DB: Processing %s.%s global' % (dbname, gl))
			if gl != 'info':			
				for i in data[gl]:
					rec = {}
					rec["msm_id"] = i
					for prop in data[gl][i]:
						rec[prop] = data[gl][i][prop]
					glob.update({'msm_id' : rec['msm_id']}, rec, upsert=True)
				if logger is not None: logger.info('UPD DB: %s.%s global: %d records processed' % (dbname, gl, len(data[gl])))
			else:
				write_dict = {}
				write_dict['$set'] = data[gl]
				write_dict['$push'] = {'updates': data[gl]['time']}
				glob.update({'station' : dbname}, write_dict, upsert=True)			
		if logger is not None: logger.info('UPD DB END')

	def get_db_info(self, dbname):
		db = self.client[dbname]
		return db['info'].find_one()

	def get_db_list(self):
		return self.client.database_names()


if __name__ == '__main__':
	pass