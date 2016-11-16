import csv
from collections import OrderedDict
from datetime import datetime
from pyspark.mllib.regression import LabeledPoint
import os

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

def preprocess_DJIA_data(file_name, sc):
	
	# Compare previous day's DJIA with today's and update
	def observe(records):
		indicators = []
		next_day = None
		for row in records:
			if next_day:
				if row[1][1] - next_day[1][0] < 0:
					indicators.append((row[0], 1))
				else:
					indicators.append((row[0], 0))
			next_day = row
		return iter(indicators)
	
	lines = sc.textFile(file_name, 1).map(lambda x: x.split(','))
	header = lines.first()	# Extract header
	processed_lines = lines.filter(lambda r: r != header).map(lambda x: (str(x[0]), \
		(float(x[1]), float(x[4])))).sortByKey(ascending=False).mapPartitions(observe)
	labels = processed_lines.map(lambda (x, y): str(x) + ',' + str(y)).coalesce(1, True)
	labels.saveAsTextFile(os.path.join(os.getcwd(), 'data/2016-03-y.train'))
	return processed_lines

def preprocess_taxi_data(file_name, sc):
	"""
	Generate taxi data in the form of 
	(date, (Passenger Count, trip time, trip distance, pickup_lon, pickup_lat, 
	dropoff_lon, dropoff_lat, payment_type, fare_amt, extra, mta_tax, 
	tip_amt, tolls_amt, total_amt))
	to provide raw data for feature extractors.
	"""
	fmt = '%Y-%m-%d %H:%M:%S'
	# Exception Handling  and removing wrong data lines 
	def isfloat(value):
		try:
			float(value)
			return True
		except:
			return False

	# remove lines if they don't have 19 values or contain bad entries (especially GPS coordinates)
	# switch around wrong lat and lon
	def remove_corrupt_data(p):
		if (len(p) == 19):
			if (isfloat(p[5]) and isfloat(p[6]) and isfloat(p[9]) and isfloat(p[10]) \
				and isfloat(p[4])):
				if (float(p[5]) != 0 and float(p[6]) != 0 and float(p[9]) != 0 and float(p[10]) != 0):
					if (float(p[9]) > 0):
						temp = p[10]
						p[9] = p[10]
						p[10] = temp
					if (float(p[5]) > 0):
						temp = p[6]
						p[5] = p[6]
						p[6] = temp	
					return p
	
	def extract_date_str(date_time_str):
		return datetime.strptime(date_time_str, fmt).strftime('%Y-%m-%d')
	
	lines = sc.textFile(file_name, 1).map(lambda x: x.split(','))
	
	header = lines.first()
	data_lines = lines.filter(lambda x: x != header).filter(remove_corrupt_data)
	full_processed_data = data_lines.map(lambda x: (str(x[2]), float(x[3]), (datetime.strptime(x[2], \
		fmt) - datetime.strptime(x[1], fmt)).seconds, float(x[4]), float(x[5]), float(x[6]), float(x[9]), \
			float(x[10]), int(x[11]), float(x[12]), float(x[13]), float(x[14]), \
				float(x[15]), float(x[16]), float(x[17]), float(x[18])))
	dated_data = full_processed_data.map(lambda x: (extract_date_str(x[0]), (x[1], x[2], x[3], x[4], \
		x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15])))
	
	# Note the pre-processed data takes following form:
	# (Key, Value)
	# Key: date string in form YYYY-MM-DD x[0]
	# Value: (Passenger Count, trip time, trip distance, pickup_lon, pickup_lat, dropoff_lon, dropoff_lat,
	# payment_type, fare_amt, extra, mta_tax, tip_amt, tolls_amt, total_amt) x[1]
	return dated_data

def generate_labeled_data(x_feature_rdd, label_rdd):
	"""
	Requires the training data in the form of (date, feat) and 
	training labels in the form of (date, indicator). Performs inner join
	on two rdds, abandon date information to generate simple (feat, indicator) pairs
	"""
	mix = x_feature_rdd.join(label_rdd).map(lambda (d, (x, y)): LabeledPoint(int(y), list(x)))
	return mix

def read_DJIA_data(sc, djia):
	data = sc.textFile(djia).map(lambda x: x.split(',')).map(lambda (x, y): (str(x), int(y)))
	return data

def read_feat(sc, ft, featureExtractor=0):
	def simple_feature(x):
		return ((str(x[0]), (float(x[1]), float(x[2]), float(x[3]), float(x[4]))))
	
	if featureExtractor == 1:
		features = sc.textFile(ft).map(lambda x: x.split(',')).map(simple_feature)
	else:
		features = sc.textFile(ft).map(lambda x: x.split(',')).map(simple_feature)
	return features

def read_res(result):
	"""
	Return a mapping from date and indicator:
	If DJIA increases this day, 1; else 0
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
