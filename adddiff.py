#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import msmjson as mjson
from msmjson import extract, get_complete_db, file_checksum, remove_file
from msmmongo import MSMMongo
from settings import MONGO_HOST as host, MONGO_PORT as port,\
	LOG_FILENAME, LOG_FORMAT, LOG_FILESIZE, LOG_FILECOUNT, LOG_TYPE,\
	ARCHIVE_DIFF_PATH, INCOMING_PATH
import logging
import logging.handlers
import os
import time
import shutil
import json

#
# Обходит все каталоги в INCOMING_PATH, находит архивы диффов и добавляет их в соответствующие базы
#

def add_diff(filename, client, logger=None):
	if logger is not None: logger.info(('ADDDIFF BEGIN (file %s)' % (filename)).center(80, '-'))
	tmp_path = '/tmp/main-msm.' + str(int(time.time())) + '/'
	if logger is not None: logger.info('ADDDIFF: Creating temporary folder %s' % tmp_path)
	try:
		if not os.path.exists(tmp_path): os.makedirs(tmp_path)
	except Exception as e:
		if logger is not None: logger.error('ADDDIFF: Error while creating tmp folder: %s' % e)
		if logger is not None: logger.info('ADDDIFF END'.center(50, '-'))
		return False
	if logger is not None: logger.info('ADDDIFF: Extracting file %s to tmp folder' % filename)
	try:
		extract(filename, tmp_path, logger=logger)
	except Exception as e:
		if logger is not None: logger.error('ADDDIFF: Error while extracting file: %s' % e)
		shutil.rmtree(tmp_path)
		if logger is not None: logger.info('ADDDIFF END'.center(50, '-'))
		return False
	if logger is not None: logger.info('ADDDIFF: Checking checksums')		
	with open(tmp_path + 'hashes.md5') as f:
		hashes = json.load(f)
	md5 = file_checksum(tmp_path + 'diff.json')
	if hashes['diff.json'] != md5:
		if logger is not None: logger.error('ADDDIFF: Incorrect checksum: %s when expecting %s' % (md5, hashes['diff.json']))
		shutil.rmtree(tmp_path)
		if logger is not None: logger.info('ADDDIFF END'.center(50, '-'))
		return False	
	if logger is not None: logger.info('ADDDIFF: Checksums OK')			

	if logger is not None: logger.info('ADDDIFF: Reading data from %s' % (tmp_path + 'diff.json'))	
	try:
		data = mjson.get_complete_db(
			tmp_path + 'diff.json',
			source='json',
			logger=logger)
	except Exception as e:
		if logger is not None: logger.error('ADDDIFF: Error while reading file: %s' % e)
		shutil.rmtree(tmp_path)
		if logger is not None: logger.info('ADDDIFF END'.center(50, '-'))
		return False
	
	dbs = client.get_db_list()
	if not data['info']['station'] in dbs:
		if logger is not None: logger.error('ADDDIFF: Error: no parent database %s' % data['info']['station'])
		shutil.rmtree(tmp_path)
		if logger is not None: logger.info('ADDDIFF END'.center(50, '-'))
		return False

	info = client.get_db_info(data['info']['station'])
	#print info
	if data['info']['prev_time'] != info['time']:
		if logger is not None: logger.error('ADDDIFF: diif\'s previous time (%s) is not equal to database actual time (%s). Some diffs may be missing.' 
			% (data['info']['prev_time'], info['time']))
		shutil.rmtree(tmp_path)
		if logger is not None: logger.info('ADDDIFF END'.center(50, '-'))
		return False		

	try:
		if logger is not None: logger.info('Adding data to MongoDB')
		client.update_db(data['info']['station'], data, logger=logger)
	except Exception as e:
		if logger is not None: logger.error('ADDDIFF: Error while operating with MongoDB: %s' % e)
		shutil.rmtree(tmp_path)
		if logger is not None: logger.info('ADDDIFF END'.center(50, '-'))
		return False

	shutil.rmtree(tmp_path)	
	archpath = ARCHIVE_DIFF_PATH + data['info']['station']
	if logger is not None: logger.info('Moving archive to %s' % archpath)
	try:
		if not os.path.exists(archpath):
			if logger is not None: logger.info('ADDDB: Creating folder %s' % archpath)
			os.makedirs(archpath)
		shutil.copy(filename, archpath)
		os.remove(filename)
	except Exception as e:
		if logger is not None: logger.error('ADDDIFF: Error while moving file: ' % e)
		if logger is not None: logger.info('ADDDIFF END'.center(50, '-'))
		return False
	if logger is not None: logger.info('ADDDIFF END'.center(50, '-'))
	return True

# ===============================================================================================================
# ===============================================================================================================
# ===============================================================================================================

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

if logger is not None: logger.info('ADDDIFF MAIN BEGIN'.center(50, '='))
try:
	if logger is not None: logger.info('ADDDIFF MAIN: Establishing connection to MongoDB server')
	mmongo = MSMMongo(host, port)
except Exception as e:
	if logger is not None: logger.error('ADDDIFF MAIN: Error while operating with MongoDB: %s' % e)
	if logger is not None: logger.info('ADDDIFF MAIN END'.center(50, '='))
	sys.exit(1)	

dirs = [i for i in os.listdir(INCOMING_PATH) if not os.path.isfile(INCOMING_PATH + i)]
for d in dirs:
	if logger is not None: logger.info('ADDDIFF MAIN: Processing directory %s' % (INCOMING_PATH + d))
	# составить список абсолютных путей файлов, которые не содержат в имени "init"
	files = ['%s%s/%s'  % (INCOMING_PATH, d, i) for i in sorted(os.listdir('%s%s/' % (INCOMING_PATH, d))) if not 'init' in i and os.path.isfile('%s%s/%s' % (INCOMING_PATH, d, i))]
	for f in files:
		if logger is not None: logger.info('ADDDIFF MAIN: Processing file %s' % (f))
		res = add_diff(f, mmongo, logger=logger)
		if res:
			if logger is not None: logger.info('ADDDIFF MAIN: file %s added to the database' % (f))
		else:
			if logger is not None: logger.error('ADDDIFF MAIN: Error while processing file %s' % (f))
			break # дальнейшие файлы, связанные с этим вокзалом, нельзя обрабатывать до устранения ошибки



if logger is not None: logger.info('ADDDIFF MAIN END'.center(50, '='))
