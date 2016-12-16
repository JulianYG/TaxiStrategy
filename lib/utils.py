import csv
from collections import Counter, defaultdict
import datetime
import os
import sys
from mathUtils import *

fmt = '%Y-%m-%d %H:%M:%S'
sfmt = '%H:%M'

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

def get_state_time_hr(time_str):
	return datetime.datetime.strptime(time_str, sfmt).strftime('%H')

def get_state_time(time_str):
	return datetime.datetime.strptime(time_str, sfmt)

def get_state_datetime(time_str):
	return datetime.datetime.strptime(time_str, fmt).strftime(sfmt)

def get_state_time_stamp(time_str, tdelta):
	old_state_time = datetime.datetime.strptime(time_str, sfmt)
	new_state_time = old_state_time + datetime.timedelta(seconds=int(tdelta * 60))
	new_state_time_hr = new_state_time.strftime('%H')
	return new_state_time, new_state_time_hr, new_state_time.strftime('%H:%M')

def time_range(start_time, end_time):
	for n in range(0, int((end_time - start_time).seconds) + 1, 60):
		yield start_time + datetime.timedelta(seconds=n)

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
					if float(p[4]) < 100 and float(p[4]) > 0 and float(p[-1]) > 0:	
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

def process_locations(locs):
	"""
	Find statistics of given list of (('lon0', 'lon1'), ('lat0', 'lat1')) grids
	Map the list of grids into a multinomial distribution with laplace smooth of 1
	"""
	grid_count_map = Counter(locs)
	total_count = sum(grid_count_map.values(), len(grid_count_map))
	for grid in grid_count_map:
		grid_count_map[grid] += 1.0
		grid_count_map[grid] /= total_count

	return grid_count_map

def enumerate_grids(boundary, grid_scale):
	lon0, lon1, lat0, lat1 = boundary
	lon_lower, lat_lower = math.floor(lon0 / grid_scale) * grid_scale, math.floor(lat0 / grid_scale) * grid_scale
	lon_higher, lat_higher = math.ceil(lon1 / grid_scale) * grid_scale, math.ceil(lat1 / grid_scale) * grid_scale
	lon_range = np.arange(lon_lower, lon_higher + grid_scale, grid_scale)
	lat_range = np.arange(lat_lower, lat_higher + grid_scale, grid_scale)
	lon_tups, lat_tups = [], []
	for i in range(len(lon_range) - 1):
		lon_tups.append((str(lon_range[i]), str(lon_range[i + 1])))
	for j in range(len(lat_range) - 1):
		lat_tups.append((str(lat_range[j]), str(lat_range[j + 1])))
	return list(itertools.product(lon_tups, lat_tups))

def get_state_reward(time, (dist_m, dist_std), (time_m, time_std), 
    (pay_m, pay_std), prob):
	return prob * ((pay_m - pay_std / 2.0) ** 2 / ((time_m - time_std / 2.0) * \
        (dist_m - dist_std / 2.0))) / time

def sort_hotspots(rdd):
	# Hot spots are the locations that have highest probabilities
	res = rdd.map(lambda ((grid, hr), ((d_m, d_v), (t_m, t_v), (p_m, p_v), 
        c_t, v, p, g)): (hr, (grid, p))).filter(lambda x: x[1][1] > 0.5)\
            .sortBy(lambda x: x[1][1], ascending=False).map(lambda x: \
                (x[0], [x[1][0]])).reduceByKey(lambda a, b: a + b)
	hotspots = res.collectAsMap()
	for hr in hotspots:
		sorted_grid_lst = hotspots[hr]
		# Only consider the top 10 hot spots in the surroundings
		if len(sorted_grid_lst) < 10:
			continue
		hotspots[hr] = sorted_grid_lst[:5] + random.sample(sorted_grid_lst[5:], 5)
	# Use some randomness to add robustness
	return hotspots

def save_params(sc, params, o):
	def counter_to_string(c):
		def tuple_to_string(tup):
			return '~'.join([str(tup[0][0][0]), str(tup[0][0][1]), str(tup[0][1][0]), 
				str(tup[0][1][1])]) + ':' + str(tup[1])
		return '|'.join(tuple_to_string(tup) for tup in c.items())
		
	txt_fmt_param = params.map(lambda ((grid, hr), ((d_m, d_v), (t_m, t_v), (p_m, p_v), c_t, v, p, g)):\
		','.join([str(grid[0][0]), str(grid[0][1]), str(grid[1][0]), str(grid[1][1]), str(hr), 
			str(d_m), str(d_v), str(t_m), str(t_v), str(p_m), str(p_v), str(c_t), str(v), str(p), 
				counter_to_string(g)])).coalesce(1, True)
	txt_fmt_param.saveAsTextFile(o)

def read_params(sc, p):
	def string_to_counter(s):
		map_dist = Counter()
		item_list = s.split('|')
		for item in item_list:
			tup = item.split(':')
			coords = tup[0].split('~')
			map_dist[((str(coords[0]), str(coords[1])), (str(coords[2]), 
				str(coords[3])))] = float(tup[1])
		return map_dist
	
	params = sc.textFile(p).map(lambda x: x.split(','))\
		.map(lambda x: ((((x[0], x[1]), (x[2], x[3])), x[4]), ((float(x[5]), float(x[6])), 
			(float(x[7]), float(x[8])), (float(x[9]), float(x[10])), float(x[11]), float(x[12]),
				float(x[13]), string_to_counter(x[14]))))
	return params

def read_policy(p):
	policy_dict = defaultdict(tuple)
	with open(p, 'r') as policy:
		reader = csv.reader(policy)
		for row in reader:
			policy_dict[(((str(row[0]), str(row[1])), (str(row[2]), str(row[3]))), 
				str(row[4]))] = ((str(row[6]), str(row[7])), (str(row[8]), str(row[9])))
	return policy_dict

def write_policy(pi, V, f):
	with open(f, 'w') as csv_file:
		writer = csv.writer(csv_file)
		for state in pi:
			writer.writerow([str(state[0][0][0]), str(state[0][0][1]), str(state[0][1][0]), 
				str(state[0][1][1]), str(state[1]), str(V[state]), pi[state][0][0], pi[state][0][1],
					pi[state][1][0], pi[state][1][1]])
			
def write_oracle(pi, f):
	with open(f, 'w') as csv_file:
		writer = csv.writer(csv_file)
		for state in pi: 
			writer.writerow([str(state[0][0][0]), str(state[0][0][1]), str(state[0][1][0]), 
				str(state[0][1][1]), str(state[1]), pi[state][0][0], pi[state][0][1],
					pi[state][1][0], pi[state][1][1]])

def write_path(p, f):
	with open(f, 'w') as fp:
		writer = csv.writer(fp)
		plan, avg_profits = p
		writer.writerow(['********', str(avg_profits[0]), str(avg_profits[1]), 
			str(avg_profits[0] / avg_profits[1]), '********'])
		for path in plan:
			profit, dist_cost = path['profit']
			writer.writerow(['--------', str(profit), str(dist_cost), 
				str(profit / dist_cost), '--------'])
			route = path['path']
			for locs in route:
				writer.writerow([str(locs[0][0][0][0]), str(locs[0][0][0][1]), 
					str(locs[0][0][1][0]), str(locs[0][0][1][1]), str(locs[0][1]), 
						str(locs[1]), str(locs[2])])

def read_path(f):
	path = []
	with open(f, 'r') as fp:
		reader = csv.reader(fp, delimiter=',')
		next(reader, None)
		route = {}
		for row in reader:
			if len(row) == 5:
				path.append(route)
				route = {'profit': float(row[1]), 'dist': float(row[2]), 
					'ratio': float(row[3]), 'loc': []}
			else:
				route['loc'].append((((float(row[0]), float(row[1])), 
					(float(row[2]), float(row[3]))), str(row[4]), float(row[5]), float(row[6])))
	return path[1:]


