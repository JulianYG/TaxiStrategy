from optparse import OptionParser
import sys
from oracle import oracle_test
from baseline import base_test
from lib.utils import *
from lib.classifier import *
"""
The execution script of NYC taxi data -> DJIA prediction
"""

def execute(m, f, p, w, l, d, o, i, y, t):
	"""
	Execute the command line inputs
	"""
	from pyspark import SparkContext
	sc = SparkContext(appName="taxi")
	# Learning phase combines train and testing
	if l:
		# Training phase, only train w/o testing
		train_x = preprocess_taxi_data(i, sc)
		train_y = preprocess_DJIA_data(t, sc)
		train(sc, train_x, train_y, o, debug=d)
		# Testing phase, if testing data is provided
		if f:
			test_data = preprocess_taxi_data(f, sc)
			return test(sc, test_data, preprocess_DJIA_data(y, sc), read_weights(o), debug=d)
	# If prediction phase, only do testing
	elif p:
		# Testing phase
		# Cases of baseline and oracle
		raw_data = read_file(f)
		if m == 0:
			return base_test(raw_data)
		if m == 1:
			return oracle_test(raw_data)
		# Regular case
		test_data = preprocess_taxi_data(f)
		return test(sc, test_data, read_res(y), read_weights(w), debug=d)
	sc.stop()

def read_command(argv):
	"""
	A helper function to extract mode, and input file information
	"""
	argv = OptionParser(description="Usage: given input file of one day's NYC taxi data, \
		predict the next morning's DIJA trend, +1/-1 as up/down")

	argv.add_option('-m', type=int, help="Mode selection: Baseline 0, Oracle 1", default=2)
	argv.add_option('-f', type=str, help="Input testing data file")
	argv.add_option('-i', type=str, help="Input training taxi data")
	argv.add_option('-t', type=str, help="Input training label data")
	argv.add_option('-p', type=int, help="Prediction phase", default=1)
	argv.add_option('-w', type=str, help="Input trained weights file path")
	argv.add_option('-o', type=str, help="Output trained weights file name")
	argv.add_option('-l', type=int, help="Learning phase", default=0)
	argv.add_option('-d', type=int, help="Debug mode", default=0)
	argv.add_option('-y', type=str, help="Actual case")

	arg, _ = argv.parse_args()
	return {'m': arg.m, 'f': arg.f, 'p': arg.p, 'w': arg.w, 'l': arg.l, \
		'd': arg.d, 'o': arg.o, 'i': arg.i, 'y': arg.y, 't': arg.t}

if __name__ == '__main__':

	arg = read_command(sys.argv[1:])
	execute(**arg)

