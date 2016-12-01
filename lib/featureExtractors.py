'''
Created on Nov 14, 2016

@author: JulianYGao
'''
"""
A library of feature extraction functions. The Rdd_data feed in 
is in the form of 
(date, (Passenger Count, trip time, trip distance, pickup_lon, pickup_lat, 
 dropoff_lon, dropoff_lat, payment_type, fare_amt, extra, mta_tax, surcharge,
 tip_amt, tolls_amt, total_amt))
and different feature selections are applied.
Feature extractors will combine feature values within one single day
and produce resulting RDD m x d where m is number of days, and d is number 
of feature dimensions.
"""
from operator import add
from lib.utils import *

def simple_aggregating_feature_extractor(rdd_data):
    """
    The simple feature extractor only takes account for total passenger count, 
    total trip time in seconds, total trip distance, and total fare amount.
    #0
    """
    simplified_data = rdd_data.map(lambda (date, feat): (date, (feat[0], \
        feat[1] * 1.0, feat[2], feat[-1])))
    aggregated_data = simplified_data.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1],\
        a[2] + b[2], a[3] + b[3]))
    features = aggregated_data.map(lambda (x, y): ','.join([str(x), str(y[0]), \
        str(y[1]), str(y[2]), str(y[3])])).coalesce(1, True)
    features.saveAsTextFile(os.path.join(os.getcwd(), 'data/simple_agg_features'))
    return aggregated_data

def simple_averaging_feature_extractor(rdd_data):
    """
    The simple feature extractor that takes account for average passenger count,
    average trip time in seconds, average trip distance, and average fare amount
    #1
    """
    simplified_data = rdd_data.map(lambda (date, feat): (date, (feat[0], \
        feat[1] * 1.0, feat[2], feat[-1])))

    sum_data_cnt = simplified_data.combineByKey(lambda val: (val[0], val[0] * val[0], val[1], val[1] * val[1], 
                                                             val[2], val[2] * val[2], val[3], val[3] * val[3], 1), \
        lambda x, val: (x[0] + val[0], x[1] + val[0] * val[0], x[2] + val[1], x[3] + val[1] * val[1], 
                        x[4] + val[2], x[5] + val[2] * val[2], x[6] + val[3], x[7] + val[3] * val[3], 
                        x[8] + 1), \
            lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3], x[4] + y[4], x[5] + y[5],
                          x[6] + y[6], x[7] + y[7], x[8] + y[8]))
    
    def stddev((sumX0, sumSquare0, sumX1, sumSquare1, sumX2, sumSquare2, sumX3, sumSquare3, n)):
        mean0 = sumX0 / n;
        std0 = math.sqrt((sumSquare0 - n * mean0 ** 2) / n)
        mean1 = sumX1 / n;
        std1 = math.sqrt((sumSquare1 - n * mean1 ** 2) / n)
        mean2 = sumX2 / n;
        std2 = math.sqrt((sumSquare2 - n * mean2 ** 2) / n)
        mean3 = sumX3 / n;
        std3 = math.sqrt((sumSquare3 - n * mean3 ** 2) / n)
        return mean0, std0, mean1, std1, mean2, std2, mean3, std3
    
    mean_std_data = sum_data_cnt.mapValues(lambda x: stddev(x))
     
    features = mean_std_data.map(lambda (x, y): ','.join([str(x), str(y[0]), \
        str(y[1]), str(y[2]), str(y[3]), str(y[4]), str(y[5]), str(y[6]), str(y[7])])).coalesce(1, True)

    features.saveAsTextFile(os.path.join(os.getcwd(), 'data/simple_avg_std_features'))
    return mean_std_data

def baseline_feature_extractor(rdd_data):
    """
    The baseline implementation of feature extractor. Takes account for all non-indicator
    values and find for average. Generates average in form of 
    (date, (Passenger Count, trip time, trip distance, payment_type, fare_amt, extra, mta_tax, 
    tip_amt, tolls_amt, total_amt))
    #2
    """
    purified_data = rdd_data.map(lambda (date, (p_cnt, trip_time, trip_dist, pickup_lon, pickup_lat,\
        dropoff_lon, dropoff_lat, payment_type, fare_amt, extra, mta_tax, surcharge, tip_amt,\
            tolls_amt, total_amt)): (date, (p_cnt, trip_time * 1.0, trip_dist, fare_amt, extra, mta_tax, \
                surcharge, tip_amt, tolls_amt, total_amt)))
 
    # 10 features summation for average and standard deviation calculation
    sum_data_cnt = purified_data.combineByKey(lambda val: (val[0], val[0] * val[0], val[1], val[1] * val[1],
        val[2], val[2] * val[2], val[3], val[3] * val[3], val[4], val[4] * val[4], val[5], val[5] * val[5], 
        val[6], val[6] * val[6], val[7], val[7] * val[7], val[8], val[8] * val[8], val[9], val[9] * val[9], 1), 
        lambda x, val: (
            x[0] + val[0], x[1] + val[0] * val[0], x[2] + val[1], x[3] + val[1] * val[1], 
            x[4] + val[2], x[5] + val[2] * val[2], x[6] + val[3], x[7] + val[3] * val[3], x[8] + val[4], 
            x[9] + val[4] * val[4], x[10] + val[5], x[11] + val[5] * val[5], x[12] + val[6], x[13] + val[6] * val[6],
            x[14] + val[7], x[15] + val[7] * val[7], x[16] + val[8], x[17] + val[8] * val[8], x[18] + val[9],
            x[19] + val[9] * val[9], x[20] + 1), 
                lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3], x[4] + y[4], x[5] + y[5], 
                    x[6] + y[6], x[7] + y[7], x[8] + y[8], x[9] + y[9], x[10] + y[10], x[11] + y[11],
                    x[12] + y[12], x[13] + y[13], x[14] + y[14], x[15] + y[15], x[16] + y[16], x[17] + y[17],
                    x[18] + y[18], x[19] + y[19], x[20] + y[20]))
     
    # Divide sum by counts of each feature
    def stddev((sumX0, sumSquare0, sumX1, sumSquare1, sumX2, sumSquare2, sumX3, sumSquare3, 
        sumX4, sumSquare4, sumX5, sumSquare5, sumX6, sumSquare6, sumX7, sumSquare7,
            sumX8, sumSquare8, sumX9, sumSquare9, n)):
        mean0 = sumX0 / n;
        std0 = math.sqrt((sumSquare0 - n * mean0 ** 2) / n)
        mean1 = sumX1 / n;
        std1 = math.sqrt((sumSquare1 - n * mean1 ** 2) / n)
        mean2 = sumX2 / n;
        std2 = math.sqrt((sumSquare2 - n * mean2 ** 2) / n)
        mean3 = sumX3 / n;
        std3 = math.sqrt((sumSquare3 - n * mean3 ** 2) / n)
        mean4 = sumX4 / n;
        std4 = math.sqrt((sumSquare4 - n * mean4 ** 2) / n)
        mean5 = sumX5 / n;
        std5 = math.sqrt((sumSquare5 - n * mean5 ** 2) / n)
        mean6 = sumX6 / n;
        std6 = math.sqrt((sumSquare6 - n * mean6 ** 2) / n)
        mean7 = sumX7 / n;
        std7 = math.sqrt((sumSquare7 - n * mean7 ** 2) / n)
        mean8 = sumX8 / n;
        std8 = math.sqrt((sumSquare8 - n * mean8 ** 2) / n)
        mean9 = sumX9 / n;
        std9 = math.sqrt((sumSquare9 - n * mean9 ** 2) / n)
        return mean0, std0, mean1, std1, mean2, std2, mean3, std3, mean4, std4,\
            mean5, std5, mean6, std6, mean7, std7, mean8, std8, mean9, std9
     
    baseline_data = sum_data_cnt.mapValues(lambda x: stddev(x))
 
    features = baseline_data.map(lambda (x, y): ','.join([str(x), str(y[0]), \
        str(y[1]), str(y[2]), str(y[3]), str(y[4]), str(y[5]), str(y[6]), str(y[7]), str(y[8]),\
            str(y[9]), str(y[10]), str(y[11]), str(y[12]), str(y[13]), str(y[14]), str(y[15]), str(y[16]), 
            str(y[17]), str(y[18]), str(y[19])])).coalesce(1, True)
  
    features.saveAsTextFile(os.path.join(os.getcwd(), 'data/baseline_features'))
    return baseline_data

def grid_feature_extractor(rdd_data):
    """
    The fine feature extractor that gridifies NYC and calculates passenger pickup/dropoff 
    rate for each grid. This is the first feature extractor that takes account of geographic 
    information. This feature extractor only takes account of average pickup/dropoff numbers 
    for each grid in each day.
    #3
    """
    
    pass

