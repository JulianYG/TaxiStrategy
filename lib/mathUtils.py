'''
Created on Dec 3, 2016

@author: JulianYGao
'''
import math
import numpy as np

def poisson_summation(lam, t):
    s = 0.0
    for i in range(t + 1):
        s += lam ** i * math.exp(-lam) / math.factorial(i)
    return s

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    return 3959.0 * 2 * math.asin(math.sqrt(a))    # Radius of earth in miles

def sigmoid(x, k, m):
    return 2 - 2 / (1.0 + math.exp(-k * (x - m)))

