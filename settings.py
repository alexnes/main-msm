#!/usr/bin/env python
# -*- coding: utf-8 -*-

MONGO_HOST='localhost'
MONGO_PORT=27017
MONGO_DBNAMES=['140100', '142100', '142200', '142300', '142400', '142500', 
'142800', '143400', '145000', '145100', '145200', '146100', '146150', '146200', 
'146300', '146400', '146500', '146550', '146600', '146700', '146900', '147000']

LOG_TYPE='both' # FILE SCREEn BOTH NONE
LOG_FILENAME='main-msm.log'
LOG_FORMAT=u'[%(asctime)s] %(levelname)-8s  %(message)-65s #%(filename)s[LINE:%(lineno)d]'
#LOG_FORMAT=u'%(message)-80s %(levelname)-8s'
LOG_FILESIZE=1024000
LOG_FILECOUNT=5


MSM_GLOBALS=['B1', 'B4', 'B9', 'B11', 'B12', 'B13', 'B14', 'B15', 'B18', 'B19',
'B27', 'B28', 'B33', 'B34', 'B35', 'B36', 'B37', 'B38', 'B39', 'B40', 'B41', 'B42',
'B43', 'B44', 'B45', 'B46', 'B47', 'B48', 'B49', 'B50', 'B51', 'B52', 'B53', 'B54']
LIB_PATH='/ZDOS/C/lib/'
DIFF_ARCHIVE_PATH='/home/admmsm/db/diffs/'
DB_ARCHIVE_MAX_AGE=5
DIFF_ARCHIVE_MAX_AGE=10

if __name__ == '__main__':
	pass