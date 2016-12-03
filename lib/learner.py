'''
Created on Nov 28, 2016
 
@author: JulianYGao
'''
from lib.utils import *
from operator import add

def get_expected_waiting_time(data):
    """
    Calculate the expected waiting time for each (hr, grid) key. 
    This value denotes the average waiting time in each grid (in minutes),
    each hour during given week day. This value can be viewed
    as lambda for the passenger arrival Poisson process.
    Calculated by sum(pickup_tdelta) / # pickups for each (hr, grid) 
    Returns (grid, hr), (expected_waiting_time, avg_number_of_pickups)
    """
    # Sort first so when merging keys, no need to generate list again
    grid_time = data.map(lambda x: ((x[0][1], get_time_stamp_date(x[0][0]), 
        get_time_stamp_hr(x[0][0])), x[0][0])).sortBy(lambda t: t[1][0])  
        
    # Now map from ((grid, date, hr), avg_tdelta) to ((grid, hr), avg_tdelta, # pickup)
    avg_grid_time_tdelta = grid_time.map(lambda ((grid, date, hr), time): ((grid, date, hr), (time, 0.0, 1.0)))\
        .reduceByKey(lambda a, b: (b[0], a[1] + get_tdelta(b[0], a[0]), a[2] + 1))\
            .map(lambda (k, v): ((k[0], k[2]), (v[1] / v[2], v[2])))
    
    # Finally average again by dates
    expected_grid_time_tdelta = avg_grid_time_tdelta.combineByKey(lambda val: (val[0], val[1], 1), lambda x,
        val: (x[0] + val[0], x[1] + val[1], x[2] + 1), lambda x, y: (x[0] + y[0], x[1] + y[1], 
            x[2] + y[2])).map(lambda (k, (s, n, c)): (k, s / c, n / c))
    
    expected_grid_time_tdelta.saveAsTextFile('data/time')
    return expected_grid_time_tdelta
    
def get_average_speed(data):
    """
    Calculate the average speed of entire map during given hour
    """
    hr_speed = data.map(lambda (k, v): (get_time_stamp_hr(k[0]), v[2])).combineByKey(lambda val:\
        (val, 1), lambda x, val: (x[0] + val, x[1] + 1), lambda x, y: (x[0] + y[0], x[1] + y[1]))\
            .map(lambda (k, (s, c)): (k, s / c))
    hr_speed.saveAsTextFile('data/speed')
    return hr_speed

def get_congestion_factor(sc, data, dayNum):
    """
    Get the congestion factor for each (grid, hr). 
    Calculated by scaling with total number of pickup and drop off in one (grid, hr).
    
    """
    hr_pickup_grid = data.map(lambda (k, v): ((get_time_stamp_hr(k[0]), k[1]), 1.0))
    hr_dropoff_grid = data.map(lambda (k, v): ((add_time(k[0], v[1]), v[3]), 1.0))\
        .filter(lambda (k, v): get_time_stamp_weekday(k[0]) == dayNum)\
            .map(lambda (k, v): ((get_time_stamp_hr(k[0]), k[1]), v))
    
    grid = sc.union([hr_pickup_grid, hr_dropoff_grid])
    total_hr_act = grid.map(lambda (k, v): (k[0], v)).reduceByKey(add)
    grid_hr_act = grid.reduceByKey(add).map(lambda ((hr, grid), cnt): (hr, (grid, cnt)))
    avg_hr_grid_cnt = total_hr_act.join(grid_hr_act)\
        .map(lambda (hr, (total, (grid, cnt))): ((grid, hr), cnt / total))
    
    def standard_deviation((sum, sumSq, n)):
        mean = sum / n
        stddev = math.sqrt((sumSq - n * mean ** 2) / n)
        return (mean, stddev)
        
    hr_avg_std = avg_hr_grid_cnt.map(lambda (k, v): (k[1], v))\
        .combineByKey(lambda val: (val, val * val, 1), lambda x, val: (x[0] + val, x[1] + val * val, 
            x[2] + 1), lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + 1))\
                .mapValues(standard_deviation)

    hr_avg_std.saveAsTextFile('data/stat')
#     congestion_factor_map = 

    
def get_dropoff_payment_distribution(data):
    pass


