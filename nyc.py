from optparse import OptionParser
import sys
from oracle import oracle_test
from baseline import base_test
from lib.utils import *
from lib.classifier import *

def execute(m, f, p, w, l, d, o, i):
	"""
	Execute the command line inputs
	"""
	# Learning phase combines train and testing
	if l:
		# Training phase, only train w/o testing
		train_data = preprocess(read_file(i), debug=d)
		train(train_data, o, debug=d)
		# Testing phase, if testing data is provided
		if f:
			test_data = preprocess(read_file(f), debug=d)
			return test(test_data, read_weights(o), debug=d)
		return
	# If prediction phase, only do testing
	if p:
		# Testing phase
		test_data = preprocess(read_file(f), debug=d)
		# Cases of baseline and oracle
		if m == 0:
			return base_test(test_data)
		if m == 1:
			return oracle_test(test_data)
		# Regular case
		return test(test_data, read_weights(w), debug=d)

def read_command(argv):
	"""
	A helper function to extract mode, and input file information
	"""
	argv = OptionParser(description="Usage: given input file of one day's NYC taxi data, \
		predict the next morning's DIJA trend, +1/-1 as up/down")

	argv.add_option('-m', type=int, help="Mode selection: Baseline 0, Oracle 1", default=2)
	argv.add_option('-f', type=str, help="Input testing data file")
	argv.add_option('-i', type=str, help="Input training data file")
	argv.add_option('-p', type=int, help="Prediction phase", default=1)
	argv.add_option('-w', type=str, help="Input trained weights file path")
	argv.add_option('-o', type=str, help="Output trained weights file name")
	argv.add_option('-l', type=int, help="Learning phase", default=0)
	argv.add_option('-d', type=int, help="Debug mode", default=0)
	arg, _ = argv.parse_args()

	return {'m': arg.m, 'f': arg.f, 'p': arg.p, 'w': arg.w, 'l': arg.l, 'd': arg.d, 'o': arg.o, 'i': arg.i}

if __name__ == '__main__':

	arg = read_command(sys.argv[1:])
	execute(**arg)

