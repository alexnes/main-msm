#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime

client = MongoClient('localhost', 27017)
db = client['a145000']
for i in db['B11'].find({'27' : {'$lt' : datetime.now(), '$gt': datetime(1901, 1, 1)}}):
	print i