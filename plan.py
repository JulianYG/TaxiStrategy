from optparse import OptionParser
from lib.simulator import Simulator
from lib.utils import *
from policy import *

def run(sc, d, i, o, t, p, g, m, f):

	db = read_params(sc, d)
	route_planner = Simulator(db, t, 80, g)

	# Let's start from Empire State building (as home address)
	home_location = (-73.985664, 40.748441)

	if not m:
		pi = read_policy(p)
	elif m == 1:
		pi = generate_random_policy(route_planner.get_states(), g)
	elif m == 2:
		# Use 0, assuming Tuesdays
		trip_info = [preprocess_taxi_data(data_file, 1, g, sc)\
			.map(lambda (k, v): ((v[3], get_state_time_stamp(get_state_datetime(k[0]), v[1])[2]), 
				[(v[4]**2 / (v[1] * v[2]), (k[1], get_state_datetime(k[0])))]))\
					.reduceByKey(lambda a, b: a + b) for data_file in f]
			# map to a ratio: profit^2 / (time * dist) to maximize utility
			# also keep track of (dropoff loc, dropoff time) state
			# Note this is mapped in the reversed way to get info easier
			# key is dropoff state, so we can get the previous state as 
			# needed in DP alg
		full_trip_info = sc.union(trip_info)
		pi = generate_oracle_policy(route_planner.get_states(), gridify(home_location[0], 
			home_location[1], g), full_trip_info)
		write_oracle(pi, 'oracle')

	routes = route_planner.profit_estimation(pi, home_location, iters=i)
	# print plan
	write_path(routes, o)

def parse_command(argv):
	"""
	A helper function to extract mode, and input file information
	"""
	def parser_callback(option, opt, val, parser):
		setattr(parser.values, option.dest, val.split(','))
		
	argv = OptionParser(description="Usage: given rdd, policy, start time and location, \
		generate simulated taxi path")

	argv.add_option('-d', type=str, help="Input parameter RDD file path")
	argv.add_option('-o', type=str, help="Output generated route file path", default='route')
	argv.add_option('-i', type=int, help="Number of iterations", default=10)
	argv.add_option('-p', type=str, help="Input policy file path")
	argv.add_option('-t', type=str, help="Input start time, in HH:MM form")
	argv.add_option('-g', type=float, help="Grid factor", default=4)
	argv.add_option('-m', type=int, help="Running mode: 0 for reading policy file, \
		1 for baseline, 2 for oracle", default=0)
	argv.add_option('-f', type=str, action='callback', callback=parser_callback,
		help="The original raw csv file that contains all trip info")
	
	arg, _ = argv.parse_args()
	return {'d': arg.d, 'i': arg.i, 'o': arg.o, 'm': arg.m, 
		't': arg.t, 'p': arg.p, 'g': arg.g, 'f': arg.f}

if __name__ == '__main__':
	from pyspark import SparkContext, SparkConf
	arg = parse_command(sys.argv[1:])
	conf = SparkConf().setAppName('taxi')
	sc = SparkContext(conf=conf)
	run(sc, **arg)
	sc.stop()



