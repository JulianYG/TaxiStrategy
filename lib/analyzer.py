# '''
# Created on Nov 24, 2016
# 
# @author: JulianYGao
# '''
# from lib.utils import generate_labeled_data
# from lib.featureExtractors import *
# from pyspark.mllib.classification import *
# from pyspark.mllib.tree import RandomForest, RandomForestModel
# 
# def test(sc, test_data, weight_dest, result_dest, test_method, debug=0, featureExtractor=0):
#     
#     
#     model = SVMModel.load(sc, weight_dest);
#     
#     testLabelsAndPreds = test_data.map(lambda p: (p.label, model.predict(p.features)))
#     
#     testLabelsAndPreds.saveAsTextFile(result_dest)
