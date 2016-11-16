from optparse import OptionParser
from oracle import oracle_test
from baseline import base_test
from lib.utils import *
from lib.classifier import *

"""
The execution script of NYC taxi data -> DJIA prediction
"""
def execute(sc, m, f, p, w, l, d, o, x, y, r):
	"""
	Execute the command line inputs
	"""
	# Learning phase combines train and testing
	if l:
		# Training phase, only train w/o testing
		data_x = preprocess_taxi_data(x, sc)
		data_y = preprocess_DJIA_data(y, sc)
		return train(sc, data_x, data_y, o, debug=d)
	# If prediction phase, only do testing
	if p:
		# Testing phase
		# Cases of baseline and oracle
		raw_data = read_file(f)
		if m == 0:
			return base_test(raw_data)
		if m == 1:
			return oracle_test(raw_data)
		# Regular case
		test_data = preprocess_taxi_data(f)
		return test(sc, test_data, w, r, debug=d)

def read_command(argv):
	"""
	A helper function to extract mode, and input file information
	"""
	argv = OptionParser(description="Usage: given input file of one day's NYC taxi data, \
		predict the next morning's DIJA trend, 1/0 as up/down")

	argv.add_option('-m', type=int, help="Mode selection: Baseline 0, Oracle 1", default=2)
	argv.add_option('-f', type=str, help="Input testing data file")
	argv.add_option('-x', type=str, help="Input training taxi data")
	argv.add_option('-y', type=str, help="Input training label data")
	argv.add_option('-p', type=int, help="Prediction phase", default=1)
	argv.add_option('-w', type=str, help="Input trained weights file path")
	argv.add_option('-o', type=str, help="Output trained weights file name")
	argv.add_option('-l', type=int, help="Learning phase", default=0)
	argv.add_option('-d', type=int, help="Debug mode", default=0)
	argv.add_option('-r', type=str, help="Results directory")
	
	arg, _ = argv.parse_args()
	return {'m': arg.m, 'f': arg.f, 'p': arg.p, 'w': arg.w, 'l': arg.l, \
		'd': arg.d, 'o': arg.o, 'x': arg.x, 'y': arg.y, 'r': arg.r}

if __name__ == '__main__':
	from pyspark import SparkContext
	arg = read_command(sys.argv[1:])
	sc = SparkContext(appName="taxi")
	execute(sc, **arg)
	sc.stop()
