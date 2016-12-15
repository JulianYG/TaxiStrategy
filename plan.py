from lib.simulator import Simulator
from lib.utils import *
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('taxi')
sc = SparkContext(conf=conf)
db = read_params(sc, 'smpt')
policy = read_policy('results/tiny_policy.txt')

# Let's start from Empire State building
p = Simulator(db, '00:01', 80, 4)

plan, avg_profits = p.profit_estimation(policy, (-73.985664, 40.748441), iters=30)

# print plan
with open('route', 'w') as f:
	writer = csv.writer(f)
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


