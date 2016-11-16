from lib.utils import generate_labeled_data
from lib.featureExtractors import *
from pyspark.mllib.classification import *

def train_raw(sc, data_x, data_y, weight_dest, debug=0, featureExtractor=0):
	
	if featureExtractor == 1:
		feat = simple_aggregating_feature_extractor(data_x)
	elif featureExtractor == 2:
		feat = simple_aggregating_feature_extractor(data_x)
	else:
		feat = simple_aggregating_feature_extractor(data_x)
	
	train_features(sc, feat, data_y, weight_dest)

def train_features(sc, feat, labels, weight_dest):
	
	labeled_train_data = generate_labeled_data(feat, labels)	
	
	# Split data for train and validation
	train_data, val_data = labeled_train_data.randomSplit([0.7, 0.3])
	model = SVMWithSGD.train(train_data, iterations=100)
	model.save(sc, weight_dest)
	
	# Evaluating the model on training data
	trainLabelsAndPreds = train_data.map(lambda p: (p.label, model.predict(p.features)))
	trainErr = trainLabelsAndPreds.filter(lambda (v, p): v != p).count() / float(train_data.count())
	
	valLabelsAndPreds = val_data.map(lambda p: (p.label, model.predict(p.features)))
	valErr = valLabelsAndPreds.filter(lambda (v, p): v != p).count() / float(val_data.count())
	
	print("Training Error = " + str(trainErr) + "\nValidation Error = " + str(valErr))

def test(sc, test_data, weight_dest, result_dest, debug=0, featureExtractor=0):
	
	model = SVMModel.load(sc, weight_dest);
	
	testLabelsAndPreds = test_data.map(lambda p: (p.label, model.predict(p.features)))
	
	testLabelsAndPreds.saveAsTextFile(result_dest)


