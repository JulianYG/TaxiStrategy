'''
Created on Dec 13, 2016
 
@author: JulianYGao
'''
"""
The simulator for running path generation
"""
from utils import *
 
class Simulator(object):
    """

    """
    def __init__(self, db, start_time, available_time, start_grid, grid_factor):
        return

    def profit_estimation(self, policy, initial_state, info, iters=80):
        """
        Note that initial state should match with the input start state
        for MDP value iteration as well
        """
        for _ in iters:
    
            distance_dist, time_dist, pay_dist, cruise_time, v, target_pickup_prob, \
                dropoff_prob = info.lookup(initial_state)
            state = initial_state
            while True:
                next_loc = policy[state](state)
                time_dist = 3
        