import csv
from collections import OrderedDict

def read_file(file_name):
	"""
	A crude raw processor to read in test data. 
	"""
	info = OrderedDict()
	with open(file_name, 'r') as f:
		next(f, None)	# Skip header
		res = csv.reader(f, delimiter=',')
		for row in res:
			info[row[1]] = row[1:]
	return info

def preprocess(file_name):
	

	pass

def read_res(result):
	"""
	Return a mapping from date and indicator:
	If DJIA increases this day, +1; else -1
	"""
	remark = OrderedDict()
	with open(result, 'r') as f:
		next(f, None)
		res = csv.reader(f, delimiter=',')
		for row in res:
			remark[row[0]] = (row[1], row[-1])
	res = {}
	key = remark.keys()[::-1]
	val = remark.values()[::-1]
	for i in range(len(remark) - 1):
		if val[i][1] <= val[i + 1][0]:
			res[key[i]] = 1
		else:
			res[key[i]] = -1
	return res
