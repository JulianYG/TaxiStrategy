from lib.simulator import Simulator
from lib.utils import *
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('taxi')
sc = SparkContext(conf=conf)
db = read_params(sc, 'smpt').collectAsMap()
policy = read_policy('results/tiny_policy.txt')

# Let's start from Empire State building
p = Simulator(db, '00:01', 80, ((-73.985664), (40.748441)), 4)

plan = p.profit_estimation(policy, iters=1)

# print plan
with open(str(plan[0]['profit']), 'w') as f:
	writer = csv.writer(f)
	for locs in plan[0]['path']:
		writer.writerow([str(locs[0]), str(locs[1]), str(locs[2])])
