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
    # Get the avg time of pickups in one date/hr
    grid_time = data.map(lambda x: ((x[0][1], get_time_stamp_date(x[0][0]), 
        get_time_stamp_hr(x[0][0])), 1)).reduceByKey(add).map(lambda (k, v): (k, 60.0 / v))
    
    expected_waiting_time = grid_time.map(lambda ((grid, date, hr), avg): ((grid, hr), avg))\
        .combineByKey(lambda val: (val, 1), lambda x, val: (x[0] + val, x[1] + 1),
            lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda (k, (s, cnt)): (k, s / cnt))
    return expected_waiting_time
    
def get_average_speed(data):
    """
    Calculate the average speed of entire map during given hour
    """
    hr_speed = data.map(lambda (k, v): (get_time_stamp_hr(k[0]), v[2])).combineByKey(lambda val:\
        (val, 1), lambda x, val: (x[0] + val, x[1] + 1), lambda x, y: (x[0] + y[0], x[1] + y[1]))\
            .map(lambda (k, (s, c)): (k, s / c))
    return hr_speed

def get_congestion_factor(sc, data, dayNum):
    """
    Get the congestion factor for each (grid, hr). 
    Calculated by scaling with total number of pickup and drop off in one (grid, hr).
    The congestion factor is mapping from an exponential decay of clusterness to a linear
    scaling of taxi speed.
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
    
    def standard_deviation((s, ssq, n)):
        mean = s / n
        if (ssq - n * mean ** 2) / n < 0:
            stddev = 10e-20
        else:
            stddev = math.sqrt((ssq - n * mean ** 2) / n) + 10e-20
        return (mean, stddev)
        
    hr_avg_std = avg_hr_grid_cnt.map(lambda (k, v): (k[1], v))\
        .combineByKey(lambda val: (val, val ** 2, 1), lambda x, val: (x[0] + val, x[1] + val ** 2, 
            x[2] + 1), lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + 1))\
                .mapValues(standard_deviation)
                
    congestion_factor_map = avg_hr_grid_cnt.map(lambda ((grid, hr), frac): (hr, (grid, frac)))\
        .join(hr_avg_std).map(lambda (hr, ((grid, frac), (mean, dev))): ((grid, hr), 
            sigmoid(frac, mean / (2 * dev ** 2), mean))).map(lambda ((grid, hr), a):\
                ((grid, hr), 0.1) if a < 0.1 else ((grid, hr), a))
    return congestion_factor_map
    
def get_pickup_probability(waiting_time, speed, congestion_factor):
    """
    Input in forms of:
    waiting_time: ((grid, hr), t)
    speed: (hr, v)
    congestion_factor: ((grid, hr), alpha)
    Probability is calculated by sampling from a Poisson distribution, 
    whose parameter lambda is defined by lambda = waiting_time t,
    and sampled by summation of t' < T = grid_size / (v * alpha)
    """ 
    cruise_time = congestion_factor.map(lambda ((grid, hr), alpha): (hr, (grid, alpha)))\
        .join(speed).map(lambda (hr, ((grid, alpha), v)): ((grid, hr), (get_grid_size(grid) / (v * alpha), v)))
    grid_prob_map = cruise_time.join(waiting_time)\
        .map(lambda ((grid, hr), ((c_t, v), w_t)): ((grid, hr), (c_t, v, 
            poisson_summation(w_t, int(round(c_t))))))
    return grid_prob_map
    # (('grid', 'hr'), (cruise_time, v, p))
    
def get_grid_dest_info(data, save=0):
    
    def standard_deviation((sum0, sumSq0, sum1, sumSq1, sum2, sumSq2, n)):
        mean0 = sum0 / n
        if (sumSq0 - n * mean0 ** 2) / n < 0:
            stddev0 = 10e-20
        else:
            stddev0 = math.sqrt((sumSq0 - n * mean0 ** 2) / n) + 10e-20
        mean1 = sum1 / n
        if (sumSq1 - n * mean1 ** 2) / n < 0:
            stddev1 = 10e-20
        else:
            stddev1 = math.sqrt((sumSq1 - n * mean1 ** 2) / n) + 10e-20
        mean2 = sum2 / n
        if (sumSq2 - n * mean2 ** 2) / n < 0:
            stddev2 = 10e-20
        else:
            stddev2 = math.sqrt((sumSq2 - n * mean2 ** 2) / n) + 10e-20
        return (mean0, stddev0, mean1, stddev1, mean2, stddev2)

    avg_std = data.map(lambda ((pick_t, pick_g), (d, t, v, drop_g, p)): ((get_time_stamp_hr(pick_t), 
        pick_g), (d, t, drop_g, p))).combineByKey(lambda v: (v[0], v[0] ** 2, 
            v[1], v[1] ** 2, [v[2]], v[3], v[3] ** 2, 1), lambda x, v: (x[0] + v[0], x[1] + v[0] ** 2,
                x[2] + v[1], x[3] + v[1] ** 2, x[4] + [v[2]], x[5] + v[3], x[6] + v[3] ** 2, x[7] + 1), 
                    lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3], x[4] + y[4],
                        x[5] + y[5], x[6] + y[6], x[7] + y[7])).map(lambda (k, v): ((k[1], k[0]), 
                            (standard_deviation((v[0], v[1], v[2], v[3], v[5], v[6], 
                                v[7])), process_locations(v[4]))))
    ##############################################################################
    # save the locations to see what it looks like
    if save:
        def separate_add((l1, l2, l3), (l4, l5, l6)):
            return (l1 + l4, l2 + l5, l3 + l6)
        def counter_to_string(c):
            def tuple_to_string(tup):
                return ':'.join([str(tup[0]), str(tup[1])])
            return ' '.join(tuple_to_string(tup) for tup in c.items())
             
        data.map(lambda ((pick_t, pick_g), (d, t, v, drop_g, p)): ((str(pick_g[0][0]), str(pick_g[0][1]), 
            str(pick_g[1][0]), str(pick_g[1][1]), get_time_stamp_hr(pick_t)), ([d], [t], [p]))).reduceByKey(separate_add)\
                .map(lambda (k, (d, t, p)): ','.join([str(k[0]), str(k[1]), str(k[2]), 
                    str(k[3]), str(k[4]), counter_to_string(Counter(d)), counter_to_string(Counter(t)), 
                    counter_to_string(Counter(p))])).coalesce(1, True).saveAsTextFile('countmap')
    ##############################################################################
    return avg_std
    # (('grid', 'hr'), (dist_mean, dist_var, time_mean, time_var, payment_mean, payment_var), drop off grids)

def get_params(prob_map, dist):
    """
    Note: (grid, hr) as key are both strings; values are floats, g is counter
    """
    # Finally, combine to get all parameters (maybe inefficient in some joins!)
    mix = prob_map.join(dist).map(lambda ((grid, hr), ((c_t, v, p), ((d_m, d_v, t_m, t_v, p_m, p_v), g))):\
        ((grid, hr), ((d_m, d_v), (t_m, t_v), (p_m, p_v), c_t, v, p, g)))
    return mix
    
    