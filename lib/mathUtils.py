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

def sigmoid(x, k, m):
    return 2 - 2 / (1.0 + math.exp(-k * (x - m)))

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

def skew_normal_sample(orig_mu, sigma):
    res = -1.0
    mu = orig_mu - sigma / 2
    while res <= 0.11:
        res = np.random.normal(mu, sigma, 1)
    return res[0]

def gridify(lon, lat, grid_factor):
    """
    Both small and large extremities factors will result in bad results
    """
    tile = 0.00111 * grid_factor
    lat_range = (math.floor(lat / tile), math.ceil(lat / tile))
    lat_grid = (str(lat_range[0] * tile), str(lat_range[1] * tile))
    lon_range = (math.floor(lon / tile), math.ceil(lon / tile))
    lon_grid = (str(lon_range[0] * tile), str(lon_range[1] * tile))
    return (lon_grid, lat_grid)

def centerize_grid(coords_tuple):
    # ((lon0, lon1), (lat0, lat1)) => (avg lon, avg lat)
    return ((coords_tuple[0][0] + coords_tuple[0][1]) / 2, (coords_tuple[1][0] + coords_tuple[1][1]) / 2)

def get_grid_size(g):
    # Helper function. Assume Manhattan distance
    width = haversine(float(g[0][0]), float(g[1][1]), float(g[0][1]), float(g[1][1]))
    height = haversine(float(g[0][0]), float(g[1][1]), float(g[0][0]), float(g[1][0]))
    return width + height   # in miles

def manhattan_distance(loc0, loc1):
    (lon0, lat0), (lon1, lat1) = centerize_grid(loc0), centerize_grid(loc1)
    return haversine(lon0, lat0, lon0, lat1) + haversine(lon0, lat1, lon1, lat1)



