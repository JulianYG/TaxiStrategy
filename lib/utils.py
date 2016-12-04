import csv
from collections import Counter
import datetime
import os
import sys
import numpy as np
from lib.mathUtils import *

fmt = '%Y-%m-%d %H:%M:%S'

def gridify(lon, lat, grid_factor):
	"""
	Both small and large extremities factors will result in bad results
	"""
	tile = 0.00111 * grid_factor
	lat_range = (math.floor(lat / tile), math.ceil(lat / tile))
	lat_grid = (str(lat_range[0] * tile), str(lat_range[1] * tile))
	lon_range = (math.floor(lon / tile), math.ceil(lon / tile))
	lon_grid = (str(lon_range[0] * tile), str(lon_range[1] * tile))
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

	# remove lines if they contain bad entries
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

def process_locations(locs, g):
	"""
	Find statistics of given list of (('lon0', 'lon1'), ('lat0', 'lat1')) grids
	Avg: average of the center of the grids, then fit in the grid
	"""
	avg_lon, avg_lat = 0, 0
	n = len(locs)
	for ((lon0, lon1), (lat0, lat1)) in locs:
		center = ((float(lon0) + float(lon1)) / 2, (float(lat0) + float(lat1)) / 2)
		avg_lon += center[0] / n
		avg_lat += center[1] / n
	return gridify(avg_lon, avg_lat, g)

def save_params(sc, params, o):
	txt_fmt_param = params.map(lambda ((grid, hr), ((d_m, d_v), (t_m, t_v), (p_m, p_v), c)):\
		','.join([str(grid[0][0]), str(grid[0][1]), str(grid[1][0]), str(grid[1][1]), str(hr), 
			str(d_m), str(d_v), str(t_m), str(t_v), str(p_m), str(p_v), str(c[0][0]), str(c[0][1]), 
				str(c[1][0]), str(c[1][1])])).coalesce(1, True)
	txt_fmt_param.saveAsTextFile(o)

def read_params(sc, p):
	params = sc.textFile(p).map(lambda x: x.split(','))\
		.map(lambda x: ((((x[0], x[1]), (x[2], x[3])), x[4]), ((x[5], x[6]), (x[7], x[8]), 
			(x[9], x[10]), ((x[11], x[12]), (x[13], x[14])))))
	return params

