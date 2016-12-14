from optparse import OptionParser
from lib.utils import *
from lib.extractor import *
from lib.simulator import *
from lib.strategy import *

"""
The execution script of NYC taxi data -> DJIA prediction
"""
def execute(sc, x, p, o, d, r, g):
	"""
	Execute the command line inputs
	"""
	if p: 
		rdd = read_params(sc, p)
	else:
		data = preprocess_taxi_data(x, d, g, sc)

# 		data_1 = preprocess_taxi_data('data/2016_03_x.csv', d, g, sc)
# 		data_2 = preprocess_taxi_data('data/2016_02_x.csv', d, g, sc)
# 		data_3 = preprocess_taxi_data('data/2016_01_x.csv', d, g, sc)
# 		data_4 = preprocess_taxi_data('data/2016_04_x.csv', d, g, sc)
# 		data_5 = preprocess_taxi_data('data/2016_05_x.csv', d, g, sc)
# 		data_6 = preprocess_taxi_data('data/2016_06_x.csv', d, g, sc)
# 		data_7 = preprocess_taxi_data('data/2015_12_x.csv', d, g, sc)
# 		data_8 = preprocess_taxi_data('data/2015_11_x.csv', d, g, sc)
# 		data_9 = preprocess_taxi_data('data/2015_10_x.csv', d, g, sc)
# 		data_10 = preprocess_taxi_data('data/2015_09_x.csv', d, g, sc)
# 		data_11 = preprocess_taxi_data('data/2015_08_x.csv', d, g, sc)
# 		data_12 = preprocess_taxi_data('data/2015_07_x.csv', d, g, sc)
# 		data_13 = preprocess_taxi_data('data/2015_06_x.csv', d, g, sc)
# 		data_14 = preprocess_taxi_data('data/2015_05_x.csv', d, g, sc)
# 		data_15 = preprocess_taxi_data('data/2015_04_x.csv', d, g, sc)
# 		data_16 = preprocess_taxi_data('data/2015_03_x.csv', d, g, sc)
# 		data_17 = preprocess_taxi_data('data/2015_02_x.csv', d, g, sc)
# 		data_18 = preprocess_taxi_data('data/2015_01_x.csv', d, g, sc)
# 			
# 		data = sc.union([data_1, data_2, data_3, data_4, data_5, data_6, data_7, data_8,\
# 			 data_9, data_10, data_11, data_12, data_13, data_14, data_15, data_16, data_17, data_18])
					
		time = get_expected_waiting_time(data)
		v = get_average_speed(data)
		alpha = get_congestion_factor(sc, data, d)
		prob_map = get_pickup_probability(time, v, alpha)
		distributions = get_grid_dest_info(data)
		rdd = get_params(prob_map, distributions)
		save_params(sc, rdd, o)
	
	mdp = TaxiMDP('00:01', 80, (('-73.9926','-73.98816'),('40.75032','40.75476')), g, rdd)
	valueIteration(mdp, r)

def read_command(argv):
	"""
	A helper function to extract mode, and input file information
	"""
	argv = OptionParser(description="Usage: given effective amount of NYC taxi data, learn parameters\
		by big data analysis and mining. Given current taxi location and time, generate lucrative\
		passenger hunting strategy")

	argv.add_option('-x', type=str, help="Input training taxi data")
	argv.add_option('-p', type=str, help="Input parameter file path")
	argv.add_option('-o', type=str, help="Output parameter file path")
	argv.add_option('-d', type=int, help="Weekday", default=0)	# Monday
	argv.add_option('-r', type=str, help="Results directory", default='results')
	argv.add_option('-g', type=float, help="Grid factor", default = 3)
	
	arg, _ = argv.parse_args()
	return {'x': arg.x, 'p': arg.p, 'o': arg.o,
		'd': arg.d, 'r': arg.r, 'g': arg.g}

if __name__ == '__main__':
	from pyspark import SparkContext, SparkConf
	arg = read_command(sys.argv[1:])
	conf = SparkConf().setAppName('taxi')
	sc = SparkContext(conf=conf)
	execute(sc, **arg)
	sc.stop()

