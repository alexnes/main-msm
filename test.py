#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import msmjson as mjson
from msmjson import debug_message
import msmmongo as mmongo
from pymongo import MongoClient
from settings import MONGO_HOST as host, MONGO_PORT as port, LOG_FILENAME as logfile, LOG_FORMAT as logformat, LOG_FILESIZE as filesize, LOG_FILECOUNT as count
import logging
import logging.handlers

#logging.basicConfig(format = logformat, level = logging.DEBUG, filename = logfile)

#logging.basicConfig(format = logformat)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
form = logging.Formatter(logformat)
filehandler = logging.handlers.RotatingFileHandler(logfile, maxBytes=filesize, backupCount=count)
filehandler.setFormatter(form)
logger.addHandler(filehandler)
strhandler = logging.StreamHandler(sys.stdout)
strhandler.setFormatter(form)
logger.addHandler(strhandler)


client = MongoClient(host, port)

data = mjson.get_complete_db(
	'/home/alex/python/db/db01.json',
	source='json',
	logger=logger)
diff = mjson.get_complete_db(
	'/home/alex/python/db/diff.json',
	source='json',
	logger=logger)

mmongo.add_new_db(client, '146200', data, logger=logger)
mmongo.update_db(client, '146200', diff, logger=logger)

print 'OK'


