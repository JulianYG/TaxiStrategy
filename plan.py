from optparse import OptionParser
from lib.simulator import Simulator
from lib.utils import *
from policy import *

def run(sc, d, i, o, t, p, g, m):

	db = read_params(sc, d)
	route_planner = Simulator(db, t, 80, g)

	if not m:
		pi = read_policy(p)
	elif m == 1:
		pi = generate_random_policy()
	elif m == 2:
		pi = generate_oracle_policy()

	# Let's start from Empire State building
	routes = route_planner.profit_estimation(pi, 
		(-73.985664, 40.748441), iters=i)
	# print plan
	write_path(routes, o)

def parse_command(argv):
	"""
	A helper function to extract mode, and input file information
	"""
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
	
	arg, _ = argv.parse_args()
	return {'d': arg.d, 'i': arg.i, 'o': arg.o, 'm': arg.m, 
		't': arg.t, 'p': arg.p, 'g': arg.g}

if __name__ == '__main__':
	from pyspark import SparkContext, SparkConf
	arg = parse_command(sys.argv[1:])
	conf = SparkConf().setAppName('taxi')
	sc = SparkContext(conf=conf)
	run(sc, **arg)
	sc.stop()
