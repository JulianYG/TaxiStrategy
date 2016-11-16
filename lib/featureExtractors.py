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
        feat[1], feat[2], feat[-1])))
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
        feat[1], feat[2], feat[-1])))
    sum_data_cnt = simplified_data.combineByKey(lambda val: (val[0], val[1], val[2], val[3], 1), \
        lambda x, val: (x[0] + val[0], x[1] + val[1], x[2] + val[2], x[3] + val[3], x[4] + 1), \
            lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3], x[4] + y[4]))
    averaged_data = sum_data_cnt.map(lambda (date, (x1, x2, x3, x4, cnt)): (date, \
        (x1 / cnt, x2 / cnt, x3 / cnt, x4 / cnt)))
    features = averaged_data.map(lambda (x, y): ','.join([str(x), str(y[0]), \
        str(y[1]), str(y[2]), str(y[3])])).coalesce(1, True)
    features.saveAsTextFile(os.path.join(os.getcwd(), 'data/simple_avg_features'))
    return averaged_data

def baseline_feature_extractor(rdd_data):
    """
    The baseline implementation of feature extractor. Takes account for all non-indicator
    values and find for average. Generates average in form of 
    (date, (Passenger Count, trip time, trip distance, pickup_lon, pickup_lat, 
    dropoff_lon, dropoff_lat, payment_type, fare_amt, extra, mta_tax, 
    tip_amt, tolls_amt, total_amt))
    #2
    """
    purified_data = rdd_data.map(lambda (date, (p_cnt, trip_time, trip_dist, pickup_lon, pickup_lat,\
        dropoff_lon, dropoff_lat, payment_type, fare_amt, extra, mta_tax, surcharge, tip_amt,\
            tolls_amt, total_amt)): (date, (p_cnt, trip_time, trip_dist, fare_amt, extra, mta_tax, \
                surcharge, tip_amt, tolls_amt, total_amt)))
    
    # 1 + 14 features all together
    sum_data_cnt = purified_data.combineByKey(lambda val: (val[0], val[1], val[2], val[3],\
        val[4], val[5], val[6], val[7], val[8], val[9], 1), lambda x, val: (x[0] + val[0], \
            x[1] + val[1], x[2] + val[2], x[3] + val[3], x[4] + val[4], x[5] + val[5], x[6] + val[6],\
                x[7] + val[7], x[8] + val[8], x[9] + val[9], x[10] + 1), lambda x, y: (x[0] + y[0], \
                    x[1] + y[1], x[2] + y[2], x[3] + y[3], x[4] + y[4], x[5] + y[5], x[6] + y[6], \
                        x[7] + y[7], x[8] + y[8], x[9] + y[9], x[10] + y[10]))
    
    # Divide sum by counts of each feature
    baseline_data = sum_data_cnt.map(lambda (date, (p_cnt, trip_time, trip_dist, fare_amt, \
        extra, mta_tax, surcharge, tip_amt, tolls_amt, total_amt, cnt)): (date, (p_cnt / cnt, \
            trip_time / cnt, trip_dist / cnt, fare_amt / cnt, extra / cnt, mta_tax / cnt, \
                surcharge / cnt, tip_amt / cnt, tolls_amt / cnt, total_amt / cnt)))
    
    features = baseline_data.map(lambda (x, y): ','.join([str(x), str(y[0]), \
        str(y[1]), str(y[2]), str(y[3]), str(y[4]), str(y[5]), str(y[6]), str(y[7]), str(y[8]),\
            str(y[9])])).coalesce(1, True)
    features.saveAsTextFile(os.path.join(os.getcwd(), 'data/baseline_features'))
    return baseline_data

def grid_feature_extractor(rdd_data):
    """
    The fine feature extractor that gridifies NYC and calculates passenger pickup/dropoff 
    rate for each grid. This is the first feature extractor that takes account of geographic 
    information. This feature extractor only takes account of average pickup/dropoff numbers 
    for each grid in each day.
    """
    pass
    

