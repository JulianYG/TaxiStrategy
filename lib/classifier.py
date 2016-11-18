from lib.utils import generate_labeled_data
from lib.featureExtractors import *
from pyspark.mllib.classification import *
from pyspark.mllib.tree import RandomForest, RandomForestModel

def train_raw(sc, data_x, data_y, weight_dest, debug=0, featureExtractor=0):
	
	if featureExtractor == 1:
		feat = simple_averaging_feature_extractor(data_x)
	elif featureExtractor == 2:
		feat = baseline_feature_extractor(data_x)
	else:
		feat = simple_aggregating_feature_extractor(data_x)
	
	train_features(sc, feat, data_y, weight_dest)

def train_features(sc, feat, labels, weight_dest):
	
	labeled_train_data = generate_labeled_data(feat, labels)	
	
	# Split data for train and validation
	train_data, val_data = labeled_train_data.randomSplit([0.7, 0.3])
	
	
	model = SVMWithSGD.train(train_data, iterations=12000, step=1.5, regParam=0.001)
	
# 	model.save(sc, weight_dest)
	
	# Evaluating the model on training data
	trainLabelsAndPreds = train_data.map(lambda p: (p.label, model.predict(p.features)))
	trainErr = trainLabelsAndPreds.filter(lambda (v, p): v != p).count() / float(train_data.count())
 	
	valLabelsAndPreds = val_data.map(lambda p: (p.label, model.predict(p.features)))
	valErr = valLabelsAndPreds.filter(lambda (v, p): v != p).count() / float(val_data.count())
	
	
# 	model = RandomForest.trainClassifier(train_data, numClasses=2, categoricalFeaturesInfo={},
#                                     numTrees=4, featureSubsetStrategy="auto",
#                                     impurity='gini', maxDepth=10, maxBins=16)
# 	
# 	predictions = model.predict(train_data.map(lambda x: x.features))
# 	trainLabelsAndPreds = train_data.map(lambda lp: lp.label).zip(predictions)
# 	trainErr = trainLabelsAndPreds.filter(lambda (v, p): v != p).count() / float(train_data.count())
# 	
# 	predictions = model.predict(val_data.map(lambda x: x.features))
# 	valLabelsAndPreds = val_data.map(lambda lp: lp.label).zip(predictions)
# 	valErr = valLabelsAndPreds.filter(lambda (v, p): v != p).count() / float(val_data.count())
	
	print("Training Error = " + str(trainErr)  + " out of " + str(train_data.count()) + \
		" samples\nValidation Error = " + str(valErr) + " out of " + str(val_data.count()) + " samples")

def test(sc, test_data, weight_dest, result_dest, debug=0, featureExtractor=0):
	
	model = SVMModel.load(sc, weight_dest);
	
	testLabelsAndPreds = test_data.map(lambda p: (p.label, model.predict(p.features)))
	
	testLabelsAndPreds.saveAsTextFile(result_dest)


