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
		file_list = [preprocess_taxi_data(data_file, d, g, sc) for data_file in x]
		data = sc.union(file_list)
		time = get_expected_waiting_time(data)
		v = get_average_speed(data)
		alpha = get_congestion_factor(sc, data, d)
		prob_map = get_pickup_probability(time, v, alpha)
		distributions = get_grid_dest_info(data)
		rdd = get_params(prob_map, distributions)
		save_params(sc, rdd, o)
	
	mdp = TaxiMDP('00:01', 80, (('-73.9926','-73.98816'), ('40.75032','40.75476')), g, rdd)
	valueIteration(mdp, r)

def read_command(argv):
	"""
	A helper function to extract mode, and input file information
	"""
	def parser_callback(option, opt, val, parser):
		setattr(parser.values, option.dest, val.split(','))

	argv = OptionParser(description="Usage: given effective amount of NYC taxi data, learn parameters\
		by big data analysis and mining. Given current taxi location and time, generate lucrative\
		passenger hunting strategy")

	argv.add_option('-x', type=str, action='callback', callback=parser_callback,
		help="Input training taxi data, separate by commas")
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

