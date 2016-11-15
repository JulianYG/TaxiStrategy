from lib.utils import generate_labeled_data
from lib.featureExtractors import *

def train(sc, train_x, train_y, weight_dest, debug=0):
	simple_aggregating_x = simple_aggregating_feature_extractor(train_x)
	check_labeled_data = generate_labeled_data(simple_aggregating_x, train_y)
	check_labeled_data.saveAsTextFile(weight_dest)
	print "called"

def test(sc, test_data, weights, debug=0):

	pass

def read_weights(file):

	pass