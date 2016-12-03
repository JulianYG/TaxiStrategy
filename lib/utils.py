import csv
from collections import OrderedDict
import datetime
import os
import sys
import math

fmt = '%Y-%m-%d %H:%M:%S'

def sigmoid(x, k, m):
	return 1 / (1 + math.exp(-k * (x - m)))

def gridify(lon, lat, grid_factor):
	lat_range = (math.floor(lat / 0.00333), math.ceil(lat / 0.00333))
	lat_grid = (str(lat_range[0] * 0.00333), str(lat_range[1] * 0.00333))
	lon_range = (math.floor(lon / 0.00333), math.ceil(lon / 0.00333))
	lon_grid = (str(lon_range[0] * 0.00333), str(lon_range[1] * 0.00333))
	return (lon_grid, lat_grid)

def get_time_stamp_hr(date_time_str):
	return datetime.datetime.strptime(date_time_str, fmt).strftime('%H')

def get_time_stamp_date(date_time_str):
	return datetime.datetime.strptime(date_time_str, fmt).strftime('%Y-%m-%d')

def get_time_stamp_weekday(date_time_str):
	return datetime.datetime.strptime(date_time_str, fmt).weekday()

def get_tdelta(date_time_str_latter, date_time_str_former):
	return (datetime.datetime.strptime(date_time_str_latter, fmt) - \
		datetime.datetime.strptime(date_time_str_former, fmt)).seconds / 60.0

def add_time(date_time_str, time):
	return str(datetime.datetime.strptime(date_time_str, fmt) + datetime.timedelta(seconds=int(time * 60)))

def preprocess_taxi_data(file_name, dayNum, grid_factor, sc):
	"""
	Generate taxi data in the form of 
	(date, (Passenger Count, trip time, trip distance, pickup_lon, pickup_lat, 
	dropoff_lon, dropoff_lat, payment_type, fare_amt, extra, mta_tax, 
	tip_amt, tolls_amt, total_amt))
	to provide raw data for feature extractors.
	"""
	# Exception Handling and removing wrong data lines 
	def isfloat(value):
		try:
			float(value)
			return True
		except:
			return False

	# remove lines if they don't have 19 values or contain bad entries (especially GPS coordinates)
	# switch around wrong lat and lon
	def sanity_check(p):
		if (len(p) == 19):
			if p[1] != p[2]:	# pickup time different from dropoff time
				if (isfloat(p[5]) and isfloat(p[6]) and isfloat(p[9]) and isfloat(p[10]) \
					and isfloat(p[4]) and isfloat(p[-1])):
					if float(p[4]) < 1000 and float(p[4]) > 0 and float(p[-1]) > 0:	
						# exclude anomalies (errors on mileage or charge)
						if (int(float(p[5])) == -73 or int(float(p[5])) == -74) and \
							(int(float(p[9])) == -73 or int(float(p[9])) == -74)\
							and (int(float(p[6])) == 40 or int(float(p[6]) == 41)) and \
							(int(float(p[10])) == 40 or int(float(p[10])) == 41):
							return p
	
	def preprocess(p):
		return (str(p[1]), float(p[4]), get_tdelta(p[2], p[1]), float(p[5]), 
			float(p[6]), float(p[9]), float(p[10]), float(p[18]))
		# Returns qualified lines:
		# pickup time, trip distance (miles), trip duration (minutes), pickup lon, pickup lat,
		# drop off lon, drop off lat, total pay amount
	
	text = sc.textFile(file_name, 1).map(lambda x: x.split(','))
	header = text.first()	
	lines = text.filter(lambda x: x != header).filter(sanity_check)
	
	raw_data = lines.map(preprocess).filter(lambda line: \
		get_time_stamp_weekday(line[0]) == dayNum) # dayNum
		
	processed_data = raw_data.map(lambda x: ((x[0], gridify(x[3], x[4], grid_factor)), 
		(x[1], x[2], x[1] / x[2], gridify(x[5], x[6], grid_factor), x[7])))\
			.filter(lambda v: v[1][2] < 3.3).filter(lambda w: w[1][2] > 0.01)	# apply speed limit
	
	# Note the pre-processed data takes following form:
	# (Key, Value)
	# Key: (Pick up time, pickup grid(lon, lat)) x[0]
	# Value: (Trip distance (miles), trip duration (minutes), trip average speed, dropoff grid(lon, lat), 
	# total pay amount) x[1]
	return processed_data

def read_params(sc, d):
	data = sc.textFile(d).map(lambda x: x.split(',')).map(lambda (x, y): (str(x), int(y)))
	return data

def read_feat(sc, ft, featureExtractor=0):
	def simple_feature(x):
		return ((str(x[0]), (float(x[1]), float(x[2]), float(x[3]), float(x[4]))))
	def baseline_feature(x):
		return ((str(x[0]), (float(x[1]), float(x[2]), float(x[3]), float(x[4]),\
			float(x[5]), float(x[6]), float(x[7]), float(x[8]), float(x[9]), float(x[10]))))
		
	raw_feat = sc.textFile(ft).map(lambda x: x.split(','))
	if featureExtractor == 1:
		features = raw_feat.map(simple_feature)
	elif featureExtractor == 2:
		features = raw_feat.map(baseline_feature)
	else:
		features = raw_feat.map(simple_feature)
	return features
