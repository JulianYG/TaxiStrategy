'''
Created on Dec 11, 2016

@author: JulianYGao
'''
from utils import *

class TaxiMDP(object):
    '''
    classdocs
    '''
    def __init__(self, start_time, available_time, start_grid, grid_factor, rdd, 
        discount=1.0, boundaries=(-74.021611, -73.742833, 40.616669, 40.886116)):
        '''
        Constructor
        '''
        self.t_start = start_time
        self.t_end = get_state_time_stamp(start_time, available_time)[0]
        self.g_start = start_grid
        self.gamma = discount
        self.grid_scale = 0.00111 * grid_factor
        self.traffic_info = rdd
        self.boundaries = boundaries
        
    def isEnd(self, state):
        return get_state_time(state[1]) > self.t_end
        
    def startState(self):
        return (self.t_start, self.g_start)

    def actions(self, state):
        result = [self._stay]
        # Stay in the old grid, driving around
        lon, lat = (float(state[0][0][0]), float(state[0][0][1])), \
            (float(state[0][1][0]), float(state[0][1][1]))

        # Nine actions in all possible directions or stay
        if (lon[0] - self.grid_scale) > self.boundaries[0]:
            result.append(self._move_left)
            if (lat[1] + self.grid_scale) < self.boundaries[3]:
                result.append(self._move_up_left)

        if (lat[0] - self.grid_scale) > self.boundaries[2]:
            result.append(self._move_down)
            if (lon[0] - self.grid_scale) > self.boundaries[0]:
                result.append(self._move_down_left)

        if (lat[1] + self.grid_scale) < self.boundaries[3]:
            result.append(self._move_up)
            if (lon[1] + self.grid_scale) < self.boundaries[1]:
                result.append(self._move_up_right)
        
        if (lon[1] + self.grid_scale) < self.boundaries[1]:
            result.append(self._move_right)
            if (lat[0] - self.grid_scale) > self.boundaries[2]:
                result.append(self._move_down_right)

        return result

    def prob_succ_reward(self, state, action):
        
        curr_loc = ((float(state[0][0][0]), float(state[0][0][1])), 
            (float(state[0][1][0]), float(state[0][1][1])))
        target_location = action(curr_loc)
        current_hr = get_state_time_hr(state[1])
        data = self.traffic_info.lookup((target_location, current_hr))
        if data:
            # Have to make sure initial state is inside RDD
            distance_dist, time_dist, pay_dist, cruise_time, target_pickup_prob, dropoff_prob = data
        else:
            # Just use some approximation from current location
            curr_d_dist, curr_t_dist, curr_p_dist, curr_cruise_time, curr_pickup_prob, _ = \
                self.traffic_info.lookup(state)
            distance_dist = (curr_d_dist[0] * 0.95, curr_d_dist[1] * 1.05)
            time_dist = (curr_t_dist[0] * 0.95, curr_t_dist[1] * 1.05)
            pay_dist = (curr_p_dist[0] * 0.95, curr_p_dist[1] * 1.05)
            target_pickup_prob = curr_pickup_prob * 0.75
            cruise_time = curr_cruise_time * 0.8
            
        new_time_str = get_state_time_stamp(state[1], cruise_time)[2]
        new_empty_state = (target_location, new_time_str)
        
        # If didn't pickup anyone at the target location
        pickup_prob = self.traffic_info.lookup(state)   # this prob is for not picking anyone currently
        result = [(1 - pickup_prob, new_empty_state, self._state_reward(cruise_time, distance_dist, 
            time_dist, pay_dist, target_pickup_prob))]

        # If pickup passenger at current location, consider future from there
#         for new_location in dropoff_prob.keys():
#             if new_location[0][0] > self.boundaries[0] and new_location[0][1] < self.boundaries[1]\
#                 and new_location[1][0] > self.boundaries[2] and new_location[1][1] < self.boundaries[3]:
#                 _, new_time_hr, new_time_str = get_state_time_stamp(state[1], 
#                     cruise_time + )
#                 new_trans_state = (new_location, new_time_str)
#                 dest_data = self.traffic_info.lookup((new_location, new_time_hr))
#                 if dest_data:
#                     dest_dist_dist, dest_time_dist, dest_pay_dist, _, dest_pickup_prob, _ = dest_data
#                 else:
#                     
#                 result.append((pickup_prob * dropoff_prob[new_location], new_trans_state, 
#                     self._state_reward(cruise_time + , dest_dist_dist, dest_time_dist, dest_pay_dist)))
        return result

    def discount(self):
        return self.gamma
    
    def states(self):
        # First get all grids
        grids = self.traffic_info.map(lambda (k, v): k[0]).distinct().collect()
        time, state = [], []
        for t_curr in time_range(get_state_time(self.t_start), self.t_end):
            time.append(t_curr.strftime('%H:%M'))
        # Then merge all grids and possible times
        for g in grids:
            state += zip(g, time)
        return state
    
    def _state_reward(self, time, (dist_m, dist_std), (time_m, time_std), 
        (pay_m, pay_std), prob):
        return math.exp(prob) * ((pay_m - pay_std / 2.0) ** 2 / ((time_m - time_std / 2.0) * \
            (dist_m - dist_std / 2.0)))
        
    def _stay(self, loc):
        return ((str(loc[0][0]), str(loc[0][1])), (str(loc[1][0]), str(loc[1][1])))

    def _move_left(self, loc):
        return ((str(loc[0][0] - self.grid_scale), str(loc[0][1] - self.grid_scale)), 
            (str(loc[1][0]), str(loc[1][1])))

    def _move_right(self, loc):
        return ((str(loc[0][0] + self.grid_scale), str(loc[0][1] + self.grid_scale)), 
             (str(loc[1][0]), str(loc[1][1])))

    def _move_up(self, loc):
        return ((str(loc[0][0]), str(loc[0][1])), (str(loc[1][0] + self.grid_scale), 
             str(loc[1][1] + self.grid_scale)))

    def _move_down(self, loc):
        return ((str(loc[0][0]), str(loc[0][1])), (str(loc[1][0] - self.grid_scale), 
            str(loc[1][1] - self.grid_scale)))

    def _move_up_left(self, loc):
        return ((str(loc[0][0] - self.grid_scale), str(loc[0][1] - self.grid_scale)), \
            (str(loc[1][0] + self.grid_scale), str(loc[1][1] + self.grid_scale)))

    def _move_up_right(self, loc):
        return ((str(loc[0][0] + self.grid_scale), str(loc[0][1] + self.grid_scale)), \
            (str(loc[1][0] + self.grid_scale), str(loc[1][1] + self.grid_scale)))

    def _move_down_left(self, loc):
        return ((str(loc[0][0] - self.grid_scale), str(loc[0][1] - self.grid_scale)), \
            (str(loc[1][0] - self.grid_scale), str(loc[1][1] - self.grid_scale)))

    def _move_down_right(self, loc):
        return ((str(loc[0][0] + self.grid_scale), str(loc[0][1] + self.grid_scale)), \
            (str(loc[1][0] - self.grid_scale), str(loc[1][1] - self.grid_scale)))



def valueIteration(mdp):
    
    V = {}
    states = mdp.states()
    for s in states():
        V[s] = 0.0
        
    def Q(state, action):
        return sum(prob * (reward + mdp.discount() * V[newState]) for prob, 
            newState, reward in mdp.prob_succ_reward(state, action))
    i = 0
    while True:
        newV, policy = {}, {}
        for s in states:
            if mdp.isEnd(s):
                newV[s] = 0.0
                policy[s] = None
            else:
                newV[s], policy[s] = max((Q(s, action), \
                    action) for action in mdp.actions(s))
        if max(abs(newV[s] - V[s]) for s in states) < 1e-6:
            break
        i += 1
        print '============================================\nIteration ' + str(i)
        for s in states:
            if mdp.isEnd(s):
                print '%s\t END' % s
            else:
                print '%s\t%s\t%s' % (s, policy[s], V[s])
                
def profit_estimation(policy, initial_state, info, iters=80):
    """
    Note that initial state should match with the input start state
    for MDP value iteration as well
    """
    for _ in iters:

        distance_dist, time_dist, pay_dist, cruise_time, target_pickup_prob, \
            dropoff_prob = info.lookup(initial_state)
        state = initial_state
        while True:
            next_loc = policy[state](state)
            time_dist = 
        
    
    
    