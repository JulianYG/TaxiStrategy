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
    def __init__(self, db, policy, start_time, available_time, start_grid, grid_factor):

        self.database = db
        self.policy = read_policy(policy)
        self.start_state = (start_grid, start_time)
        self.grid_factor = grid_factor
        self.end_time = get_state_time_stamp(start_time, available_time)[0]


    def profit_estimation(self, policy, initial_state, info, iters=80):
        """
        Note that initial state should match with the input start state
        for MDP value iteration as well
        """
        for _ in iters:
            curr_state = self.start_state
            total_profit, total_dist = 0.0, 0.0

            while get_state_time(curr_state[1]) < self.end_time:

                distance_dist, time_dist, pay_dist, cruise_time, v, pickup_prob, \
                    dropoff_map = info.lookup(curr_state)

                next_non_pickup_loc = self.policy[curr_state]
                # travel_time = cruise_time + manhattan_distance(dropoff_location, curr_location) / v
                # non_pickup_next_state = (next_non_pickup_loc, 
                #     get_state_time_stamp(curr_state[1], travel_time)[2])
                # non_pickup_reward = 


                # time_dist = 3
            

