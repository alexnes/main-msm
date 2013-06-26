#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import msmjson as mjson
from msmjson import extract, get_complete_db, file_checksum
from msmmongo import MSMMongo
from settings import MONGO_HOST as host, MONGO_PORT as port,\
	LOG_FILENAME, LOG_FORMAT, LOG_FILESIZE, LOG_FILECOUNT, LOG_TYPE
import logging
import logging.handlers
import os
import time
import shutil
import json

#
# Принимает в качестве аргумента tar.bz2 архив, сгенерированный скриптом createjson.py на стороне узла (node-msm).
# В архиве должны быть следующие файлы:
# db.json - JSON-дамп msm базы
# hashes.md5 - файл с md5-хешем файла db.json
#

if len(sys.argv) != 2:
	print 'Usage:\n\t%s <filename>.tar.bz2' % sys.argv[0]
	sys.exit()

if LOG_TYPE.upper() == 'NONE':
	logger = None
else:
	logger = logging.getLogger(__name__)
	logger.setLevel(logging.DEBUG)
	form = logging.Formatter(LOG_FORMAT)
	filehandler = logging.handlers.RotatingFileHandler(
		LOG_FILENAME,
		maxBytes=LOG_FILESIZE,
		backupCount=LOG_FILECOUNT)
	filehandler.setFormatter(form)
	strhandler = logging.StreamHandler(sys.stdout)
	strhandler.setFormatter(form)
	if LOG_TYPE.upper() == 'FILE':
		logger.addHandler(filehandler)
	elif LOG_TYPE.upper() == 'SCREEN':
		logger.addHandler(strhandler)
	else:
		logger.addHandler(filehandler)
		logger.addHandler(strhandler)

if logger is not None: logger.info('ADDDB BEGIN'.center(50, '-'))
tmp_path = '/tmp/main-msm.' + str(int(time.time())) + '/'
if logger is not None: logger.info('ADDDB: Creating temporary folder %s' % tmp_path)
try:
	if not os.path.exists(tmp_path): os.makedirs(tmp_path)
except Exception as e:
	if logger is not None: logger.error('ADDDB: Error while creating tmp folder: %s' % e)
	if logger is not None: logger.info('ADDDB END'.center(50, '-'))
	sys.exit(1)
if logger is not None: logger.info('ADDDB: Extracting file %s to tmp folder' % sys.argv[1])
try:
	extract(sys.argv[1], tmp_path, logger=logger)
except Exception as e:
	if logger is not None: logger.error('ADDDB: Error while extracting file: %s' % e)
	shutil.rmtree(tmp_path)
	if logger is not None: logger.info('ADDDB END'.center(50, '-'))
	sys.exit(1)	
if logger is not None: logger.info('ADDDB: Checking checksums')		
with open(tmp_path + 'hashes.md5') as f:
	hashes = json.load(f)
md5 = file_checksum(tmp_path + 'db.json')
if hashes['db.json'] != md5:
	if logger is not None: logger.error('ADDDB: Incorrect checksum: %s when expecting %s' % (md5, hashes['db.json']))
	shutil.rmtree(tmp_path)
	if logger is not None: logger.info('ADDDB END'.center(50, '-'))
	sys.exit(1)		
if logger is not None: logger.info('ADDDB: Checksums OK')			
#print hashes
if logger is not None: logger.info('ADDDB: Reading database from %s' % (tmp_path + 'db.json'))	
try:
	data = mjson.get_complete_db(
		tmp_path + 'db.json',
		source='json',
		logger=logger)
except Exception as e:
	if logger is not None: logger.error('ADDDB: Error while reading file: %s' % e)
	shutil.rmtree(tmp_path)
	if logger is not None: logger.info('ADDDB END'.center(50, '-'))
	sys.exit(1)	
try:
	if logger is not None: logger.info('Establishing connection to MongoDB server')
	mmongo = MSMMongo(host, port)
	if logger is not None: logger.info('Adding data to MongoDB')
	mmongo.add_new_db(data['info']['station'], data, logger=logger)
except Exception as e:
	if logger is not None: logger.error('ADDDB: Error while operating with MongoDB: %s' % e)
	shutil.rmtree(tmp_path)
	if logger is not None: logger.info('ADDDB END'.center(50, '-'))
	sys.exit(1)	
shutil.rmtree(tmp_path)	
if logger is not None: logger.info('ADDDB END'.center(50, '-'))
#data = mjson.get_complete_db(
#	'/home/alex/python/db/db01.json',
#	source='json',
#	logger=logger)
#diff = mjson.get_complete_db(
#	'/home/alex/python/db/diff.json',
#	source='json',
#	logger=logger)

#mmongo.add_new_db('146200', data, logger=logger)
#mmongo.update_db('146200', diff, logger=logger)

#print 'OK'