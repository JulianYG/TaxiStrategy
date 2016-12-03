from optparse import OptionParser
from oracle import oracle_test
from baseline import base_test
from lib.utils import *
from lib.learner import *
from lib.search import *
from lib.analyzer import *

"""
The execution script of NYC taxi data -> DJIA prediction
"""
def execute(sc, m, x, p, o, d, r, g):
	"""
	Execute the command line inputs
	"""
	if p: 
		data = read_params(sc, p)
	else:
		data = preprocess_taxi_data(x, d, g, sc)
# 		get_expected_waiting_time(data)
		get_congestion_factor(sc, data, d)

# 			data_2 = preprocess_taxi_data('data/2016_02_x.csv', sc)
# 			data_1 = preprocess_taxi_data(x, sc)		
		
# 		data_x = preprocess_taxi_data(x, d, g, sc)	 # original
		
# 			data_3 = preprocess_taxi_data('data/2016_01_x.csv', sc)
# 			data_4 = preprocess_taxi_data('data/2016_04_x.csv', sc)
# 			data_5 = preprocess_taxi_data('data/2016_05_x.csv', sc)
# 			data_6 = preprocess_taxi_data('data/2016_06_x.csv', sc)
#  		
# 			data_7 = preprocess_taxi_data('data/2015_12_x.csv', sc)
# 			data_8 = preprocess_taxi_data('data/2015_11_x.csv', sc)
# 			data_9 = preprocess_taxi_data('data/2015_10_x.csv', sc)
# 			data_10 = preprocess_taxi_data('data/2015_09_x.csv', sc)
# 			data_11 = preprocess_taxi_data('data/2015_08_x.csv', sc)
# 			data_12 = preprocess_taxi_data('data/2015_07_x.csv', sc)
# 			data_13 = preprocess_taxi_data('data/2015_06_x.csv', sc)
# 			data_14 = preprocess_taxi_data('data/2015_05_x.csv', sc)
# 			data_15 = preprocess_taxi_data('data/2015_04_x.csv', sc)
# 			data_16 = preprocess_taxi_data('data/2015_03_x.csv', sc)
# 			data_17 = preprocess_taxi_data('data/2015_02_x.csv', sc)
# 			data_18 = preprocess_taxi_data('data/2015_01_x.csv', sc)
		
# 			data_x = sc.union([data_1, data_2, data_3, data_4, data_5, data_6, data_7, data_8,\
# 				 data_9, data_10, data_11, data_12, data_13, data_14, data_15, data_16, data_17, data_18])

def read_command(argv):
	"""
	A helper function to extract mode, and input file information
	"""
	argv = OptionParser(description="Usage: given effective amount of NYC taxi data, learn parameters\
		by big data analysis and mining. Given current taxi location and time, generate lucrative\
		passenger hunting strategy")

	argv.add_option('-m', type=int, help="Mode selection: Baseline 0, Oracle 1", default=2)
	argv.add_option('-x', type=str, help="Input training taxi data")
	argv.add_option('-p', type=str, help="Input parameter file path")
	argv.add_option('-o', type=str, help="Output parameter file path")
	argv.add_option('-d', type=int, help="Weekday", default=0)	# Monday
	argv.add_option('-r', type=str, help="Results directory", default='results')
	argv.add_option('-g', type=float, help="Grid factor", default = 0.00333)
	
	arg, _ = argv.parse_args()
	return {'m': arg.m, 'x': arg.x, 'p': arg.p, 'o': arg.o,
		'd': arg.d, 'r': arg.r, 'g': arg.g}

if __name__ == '__main__':
	from pyspark import SparkContext
	arg = read_command(sys.argv[1:])
	sc = SparkContext(appName="taxi")
	execute(sc, **arg)
	sc.stop()
