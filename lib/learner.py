'''
Created on Nov 24, 2016

@author: JulianYGao
'''

from lib.utils import generate_labeled_data
from lib.featureExtractors import *
from pyspark.mllib.classification import *
from pyspark.mllib.tree import RandomForest, RandomForestModel

def train_raw(sc, data_x, data_y, weight_dest, train_method, debug=0, featureExtractor=0):
    
    if featureExtractor == 1:
        feat = simple_averaging_feature_extractor(data_x)
    elif featureExtractor == 2:
        feat = baseline_feature_extractor(data_x)
    else:
        feat = simple_aggregating_feature_extractor(data_x)
    
    train_features(sc, feat, data_y, weight_dest, train_method)

def train_features(sc, feat, labels, weight_dest, train_method):
    
    labeled_train_data = generate_labeled_data(feat, labels)    
    
    # Split data for train and validation
    train_data, val_data = labeled_train_data.randomSplit([0.7, 0.3])
    
    
    if train_method == "forest":
        model, trainErr, valErr = random_forest_train(train_data, val_data, numTrees=4,
            impurity='gini', maxDepth=10, maxBins=16)
    else:   # Default is SVM
        model, trainErr, valErr = svm_train(train_data, val_data, 
            iterations=8000, step=0.5, regParam=0.05)
        
    #   model.save(sc, weight_dest)  
    print("Training Error = " + str(trainErr)  + " out of " + str(train_data.count()) + \
        " samples\nValidation Error = " + str(valErr) + " out of " + str(val_data.count()) + " samples")

def svm_train(train_data, val_data, iterations=500, step=1.0, regParam=0.1):
    
    model = SVMWithSGD.train(train_data, iterations=iterations, step=step, regParam=regParam)
    
    trainLabelsAndPreds = train_data.map(lambda p: (p.label, model.predict(p.features)))
    trainErr = trainLabelsAndPreds.filter(lambda (v, p): v != p).count() / float(train_data.count())
        
    valLabelsAndPreds = val_data.map(lambda p: (p.label, model.predict(p.features)))
    valErr = valLabelsAndPreds.filter(lambda (v, p): v != p).count() / float(val_data.count())

    return model, trainErr, valErr

def random_forest_train(train_data, val_data, numTrees=3, impurity='gini', maxDepth=10, maxBins=16):
    
    model = RandomForest.trainClassifier(train_data, numClasses=2, categoricalFeaturesInfo={},
        numTrees=numTrees, featureSubsetStrategy="auto", impurity=impurity,
            maxDepth=maxDepth, maxBins=maxBins)
     
    predictions = model.predict(train_data.map(lambda x: x.features))
    trainLabelsAndPreds = train_data.map(lambda lp: lp.label).zip(predictions)
    trainErr = trainLabelsAndPreds.filter(lambda (v, p): v != p).count() / float(train_data.count())
     
    predictions = model.predict(val_data.map(lambda x: x.features))
    valLabelsAndPreds = val_data.map(lambda lp: lp.label).zip(predictions)
    valErr = valLabelsAndPreds.filter(lambda (v, p): v != p).count() / float(val_data.count())

    return model, trainErr, valErr



