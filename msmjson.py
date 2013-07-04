#!/usr/bin/env python
# -*- coding: utf-8 -*-
from sys import exc_info
import json
import datetime
import tarfile
import os
import time
from datetime import datetime
from paramiko import SSHClient, AutoAddPolicy
from scp import SCPClient
import hashlib
import logging

def debug_message(message):
	# converts the message to log-like format
	now_time = datetime.datetime.now()	
	return "%s\t %s" % (now_time.strftime("%d.%m.%Y %H:%M.%S"), message)

def print_msg(message, logger=None, loglevel=1):
	# Добавляет к сообщению message дату и время и выводит его на экран (debug == True) и/или в лог (logger != None)
	# loglevel:
	# 0 	- DEBUG
	# 1 	- INFO
	# 2 	- WARNING
	# 3 	- ERROR
	# 4 	- CRITICAL
	msg = debug_message(message)
	if debug: print msg
	if logger != None:
		if loglevel == 0:
			logger.debug(msg)
		elif loglevel == 2:
			logger.warning(msg)
		elif loglevel == 3:
			logger.error(msg)
		elif loglevel == 4:
			logger.critical(msg)
		else:
			logger.info(msg)


def get_data(file_path, debug = False):
	# Получить из файла список словарей {"par":{"id":<id>, "prop":<prop>}, "val":<val>}.
	# Разные элементы списка могут иметь одинаковое значение ["par"]["id"]
	data = []
	f = open(file_path)
	if debug: n = 0
	lines = f.readlines()
	if len(lines) > 0:
		lines = lines[1 : len(lines) - 3]

		buf = ""
		lines_new = []
		for s in reversed(lines):
			if s[0] == '{':
				if buf != "":
					s += buf
					buf = ""
				lines_new.append(s.replace('\r', '').replace('\n', '').replace('\t', '').decode('koi8_r').encode('utf-8'))
			else:
				buf += s

		for s in reversed(lines_new):
			if debug:
				n += 1				
				print "Processing line %d" % (n)
				print s
			try:
				data.append(json.loads(s))
			except ValueError:
				Type, Value, Trace = exc_info()
				if debug:
					print Value	
	if debug: return data, getsizeof(data),
	else: return data

def get_db(file_path):
	# Возвращает словарь {"<id>":{"<prop1>": <val1>, "<prop2>": <val2>, ...}}.
	data = {}
	f = open(file_path)
	lines = f.readlines()
	if len(lines) > 0:
		lines = lines[1 : len(lines) - 3]

		buf = ""
		lines_new = []
		for s in reversed(lines):
			if s[0] == '{':
				if buf != "":
					s += buf
					buf = ""
				lines_new.append(s.replace('\r', '').replace('\n', '').replace('\t', '').decode('koi8_r').encode('utf-8'))
			else:
				buf += s

		for s in reversed(lines_new):
			try:
				line = json.loads(s)
				if data.get(line["par"]["id"]):
					data[line["par"]["id"]][line["par"]["prop"]] = line["val"]
				else:
					data[line["par"]["id"]] = {}
					data[line["par"]["id"]][line["par"]["prop"]] = line["val"]					
			except ValueError:
				Type, Value, Trace = exc_info()
	return data

def normalize_time(data):
	# Находит в данных свойства, содержащие дату и/или время (свойства 27, 28, 36, 71, 72, 130, 131)
	# и переводит их из строкового формата в формат time.struct_time
	time_props = [u'27', u'28', u'71']
	date_props = [u'36', u'130', u'131']
	datetime_props = [u'72']
	for gl in data:
		for rec in data[gl]:
			for prop in data[gl][rec]:
				#print '====================================================================='
				#print data[gl][rec][prop] + ' ====>',
				if prop in time_props:
					data[gl][rec][prop] = datetime.fromtimestamp(time.mktime(time.strptime(data[gl][rec][prop], "%H:%M")))
				elif prop in date_props:
					data[gl][rec][prop] = datetime.fromtimestamp(time.mktime(time.strptime(data[gl][rec][prop], "%d.%m.%y")))
				elif prop in datetime_props:
					data[gl][rec][prop] = datetime.fromtimestamp(time.mktime(time.strptime(data[gl][rec][prop], "%H:%M %d.%m.%y")))
				#print data[gl][rec][prop]


def get_complete_db(
	path,
	source="FILE",
	msm_globals=('B1', 'B4', 'B9', 'B11', 'B12', 'B13', 'B14', 'B15', 'B18', 'B19', 'B27', 'B28',
	'B33', 'B34', 'B35', 'B36', 'B37', 'B38', 'B39', 'B40',	'B41', 'B42', 'B43', 'B44', 'B45',
	'B46', 'B47', 'B48', 'B49', 'B50', 'B51', 'B52', 'B53', 'B54'),
	info = {},
	logger=None):
	# Возвращает словарь, содержащий данные из всех (по умолчанию) или выбранных глобалей (msm_globals).
	# {"<global_name>": {"<id>": {"<prop1>": <val1>, "<prop2>": <val2>, ... }, ... }, ... }
	# В качестве источника данных выступает каталог с данными (path), выгруженными из MSM (source="FILE"),
	# каждая глобаль представлена в каталоге отдельным файлом с именем глобали,
	# или дамп полной базы в JSON-формате (source="JSON"), при этом в path передается полный путь к файлу
	data={}	
	if source.upper() == "FILE":
		if path[len(path) - 1] != '/': path += '/'
		for g in msm_globals:
			if logger is not None: logger.info("READ: processing file: %s%s" % (path, g))
			data[g]= get_db(path + g)
		data["info"] = info
	elif source.upper() == "JSON":
		if logger is not None: logger.info("READ: processing file: %s" % (path))
		with open(path) as f:
		    data = json.load(f)	
	if logger is not None: logger.info("READ: done.")
	normalize_time(data)
	return data

def dump_db(db, filename, logger=None):
	# Сохраняет словарь db в файл filename в формате JSON
	if logger is not None: logger.info("DUMP: processing file: %s" % (filename))
	with open(filename, 'w') as f:
		json.dump(db, f)	
	if logger is not None: logger.info("DUMP: done.")

def getdiff(db2, db1):
	# Возвращает словарь с такой же структурой, как и у db1 и db2. Словарь содержит записи, которые есть в db2 и отсутствуют в db1,
	# или те, которые присутствуют в db2 в измененном виде.
	diff = {}
	for global_name in db2:
		if db1.get(global_name) != None:
			for id2 in db2[global_name]:
				if db1[global_name].get(id2) == None:
					if diff.get(global_name) == None:
						diff[global_name] = {}
					diff[global_name][id2] = db2[global_name][id2]
				else:
					if db1[global_name][id2] != db2[global_name][id2]:
						if diff.get(global_name) == None:
							diff[global_name] = {}
						diff[global_name][id2] = db2[global_name][id2]
		else:
			diff[global_name] = db2[global_name]
	return diff
	
def file_checksum(filePath):
	with open(filePath, 'rb') as fh:
		m = hashlib.md5()
		while True:
			data = fh.read(8192)
			if not data:
				break
			m.update(data)
		return m.hexdigest()

def compress(archive=None, files=None, crop_names=False, md5=False, logger=None):
	# Создает archive (tar.bz2 архив) и добавляет в него файлы files
	hashes = {}
	if archive == None or files == None: pass
	tar = tarfile.open(archive, "w:bz2")
	for name in files:
		arcname = os.path.basename(name)
		if logger is not None: logger.info("COMPRESS: processing file: %s" % (name))
		if md5: hashes[arcname] = file_checksum(name)
		if crop_names:
			tar.add(name, arcname=arcname)
		else:
			tar.add(name)
	if md5:
		with open('hashes.md5', 'w') as f:
			json.dump(hashes, f)
		tar.add('hashes.md5')
	tar.close()	
	if md5: os.remove('hashes.md5')
	if logger is not None: logger.info("COMPRESS: %s done" % (archive))

def extract(archive=None, path=".", logger=None):
	# Извлекает все файлы из архива archive в каталог path
	if archive == None: pass
	if logger is not None: logger.info("EXTRACT: processing file: %s to %s" % (archive, path))
	try:
		tar = tarfile.open(archive, "r:bz2")
		tar.extractall(path)
		tar.close()		
	except Exception as e:
		if logger is not None: logger.error("EXTRACT: error: %s" % (e))
	if logger is not None: logger.info("EXTRACT: %s done." % (archive))

def dump_compressed_db(db, filename, dbname="db.json", md5=False, logger=None):
	# Сохраняет словарь db в сжатый tar.bz2 файл filename в формате JSON.
	#dbname = "/tmp/" + dbname
	if logger is not None: logger.info("DUMP_C: processing file: %s" % (dbname))
	with open(dbname, 'w') as f:
		json.dump(db, f)	
	if logger is not None: logger.info("DUMP_C: compressing dump: %s" % (filename))
	compress(filename, [dbname], crop_names=True, md5=md5)
	if logger is not None: logger.info("DUMP_C: removing temporary %s" % (dbname))	
	os.remove(dbname)
	if logger is not None: logger.info("DUMP_C: done.")

def get_file(remote_file, local_path, ip, username, password, logger=None):
	# Получает с удаленной машины файл remote_file с помощью scp и сохраняет его в local_path.
	if local_path[len(local_path) - 1] != '/': local_path += '/'
	ssh = SSHClient()
	ssh.set_missing_host_key_policy(AutoAddPolicy())
	ssh.load_system_host_keys()
	if logger is not None: logger.info("SCP GET: connecting to %s" % (ip))
	try:
		ssh.connect(ip, username=username, password=password)
	except:
		if logger is not None: logger.info("SCP GET: failed to connect to %s" % (ip))
		return False
	else:
		if logger is not None: logger.info("SCP GET: connected to %s" % (ip))
	try:
		if logger is not None: logger.info("SCP GET: retrieving file %s" % (remote_file))
		scp = SCPClient(ssh.get_transport())
		scp.get(remote_file, local_path)
	except:
		if logger is not None: logger.error("SCP GET: error: failed to retrieve file %s" % (remote_file))
		ssh.close()
		return False
	else:
		if logger is not None: logger.info("SCP GET: file saved to %s folder" % (local_path))
	ssh.close()
	return True

def send_file(local_file, remote_path, ip, username, password, logger=None):
	# Отсылает файл local_file в remote_path удаленной машины по scp
	if remote_path[len(remote_path) - 1] != '/': remote_path += '/'
	ssh = SSHClient()
	ssh.set_missing_host_key_policy(AutoAddPolicy())
	ssh.load_system_host_keys()
	if logger is not None: logger.info("SCP SEND: connecting to %s" % (ip))
	try:
		ssh.connect(ip, username=username, password=password)
	except:
		if logger is not None: logger.info("SCP SEND: failed to connect to %s" % (ip))
		return False
	else:
		if logger is not None: logger.info("SCP SEND: connected to %s" % (ip))
	try:
		if logger is not None: logger.info("SCP SEND: sending file %s" % (local_file))
		scp = SCPClient(ssh.get_transport())
		scp.put(local_file, remote_path)
	except:
		if logger is not None: logger.error("SCP SEND: error: failed to send file %s" % (local_file))
		ssh.close()
		return False
	else:
		if logger is not None: logger.info("SCP SEND: file sent to %s@%s:%s " % (username, ip, remote_path))
	ssh.close()		
	return True
def fileage(path):
	# Возвращает возраст файла в днях
	t = os.path.getmtime(path)
	d = datetime.datetime.now() - datetime.datetime.fromtimestamp(t)	
	return d.days

def remove_old_files(path, age, logger=None):
	# Удаляет в каталоге path все файлы старше age (в днях)
	if path[len(path) - 1] != '/': path += '/'
	for f in os.listdir(path):
		if fileage(path + f) > age:
			try:
				os.remove(path + f)
			except:
				if logger is not None: logger.error("CLEAR: error: failed to remove file %s " % (path + f))
			else:
				if logger is not None: logger.info("CLEAR: removed file %s " % (path + f))

def remove_all_files(path, logger=None):
	# Удаляет в каталоге path все файлы старше age (в днях)
	if path[len(path) - 1] != '/': path += '/'
	for f in os.listdir(path):
		try:
			os.remove(path + f)
		except:
			if logger is not None: logger.error("CLEAR: error: failed to remove file %s " % (path + f))
		else:
			if logger is not None: logger.info("CLEAR: removed file %s " % (path + f))

def remove_file(path, logger=None):
	# Удаляет файл path 
	try:
		os.remove(path)
	except:
		if logger is not None: logger.error("CLEAR: error: failed to remove file %s " % (path))
	else:
		if logger is not None: logger.info("CLEAR: removed file %s " % (path))

if __name__ == '__main__':
	pass

	# ВАЖНО !!!
	# К функциям приделано полноценное логирование вместо вывода debug_message в stdout.
	# Во всех, функциях где это поддерживается, параметр debug=False заменен на logger=None.
	# Теперь, при необходимости вести логи, в начале скрипта создается объект-логгер:
	
	#import logging
	#import logging.handlers
	#logger = logging.getLogger(__name__)
	#logger.setLevel(logging.DEBUG)
	#form = logging.Formatter(u'[%(asctime)s] %(levelname)-8s  %(message)-65s #%(filename)s[LINE:%(lineno)d]')
	#filehandler = logging.handlers.RotatingFileHandler('log.log', maxBytes=102400, backupCount=5)
	#filehandler.setFormatter(form)
	#logger.addHandler(filehandler)
	#strhandler = logging.StreamHandler(sys.stdout)
	#strhandler.setFormatter(form)
	#logger.addHandler(strhandler)

	# Будет создан логгер, который будет посылать строки в стандартный вывод 
	# и вести циклический лог из 5 файлов размером не более 100 кб каждый.


	#data1 = get_complete_db("/home/alex/ZDOS/C/savedm", source="file", logger=logger)
	#dump_db(data1, "dbm.json", logger=logger)
	#data2 = get_complete_db("db02.json", source="json", logger=logger)
	#dump_compressed_db(data2, "saved/db02.tar.bz2", "db2.json", logger=logger)
	#diff2 = getdiff(data2, data1)
	#dump_db(diff2, "diff2-1.json", logger=logger)
	#compress("dbm.tar.bz2", ["dbm.json"], logger=logger)
	#extract("diff.tar.bz2", "saved/", logger=logger)
	#get_file("/home/admmsm/146200.tar", "saved", "192.168.20.2", "admmsm", "msm-bus", logger=logger)

	#data = {}
	#for i in xrange(0,4):
	#	data[i] = get_complete_db("/home/alex/ZDOS/C/saved%d" % (i), source="file", logger=logger)
	#	dump_db(data[i], "db%02d.json" % (i), logger=logger)

	#get_file("/home/admmsm/146200.tar", "saved", "192.168.20.2", "admmsm", "msm-bus", logger=logger)
	#from globals_description import description
	#print description
	#send_file("db00.json", "/home/admmsm/", "192.168.1.1", "admmsm", "msm-bus", logger=logger)
	#remove_old_files('/home/alex/tmp/', 1, logger=logger)
	#remove_all_files('/home/alex/tmp/', logger=logger)
