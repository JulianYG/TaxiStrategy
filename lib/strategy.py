'''
Created on Dec 11, 2016

@author: JulianYGao
'''
from utils import *

class TaxiMDP(object):
    '''
    classdocs
    '''
    def __init__(self, start_time, available_time, start_grid, grid_factor, rdd, discount=1.0):
        '''
        Constructor
        '''
        self.t_avail = available_time
        self.t_start = start_time
        self.g_start = start_grid
        self.gamma = discount
        self.grid_factor = grid_factor
        self.traffic_info = rdd
        
    def isEnd(self, state):
        return get_state_time_stamp(state[1]) > get_start_end_time_stamp(\
            get_state_time_stamp(self.t_start), self.t_avail)
        
    def startState(self):
        return (self.t_start, self.g_start)
    
    def actions(self, state):
        # Nine actions in all possible directions or stay
        result = []
        grid_scale = 0.0111 * self.grid_factor
        move_left_loc = ((state[0][0][0] - grid_scale, state[0][0][1] - grid_scale), 
            (state[0][1][0], state[0][1][1]))
        move_right_loc = ((state[0][0][0] + grid_scale, state[0][0][1] + grid_scale), 
            (state[0][1][0], state[0][1][1]))
        
    def discount(self):
        return self.gamma
        
        